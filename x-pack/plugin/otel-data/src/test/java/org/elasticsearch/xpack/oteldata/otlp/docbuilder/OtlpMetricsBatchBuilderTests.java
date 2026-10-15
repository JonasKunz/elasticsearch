/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class OtlpMetricsBatchBuilderTests extends ESTestCase {

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Writes {@code blocks} blocks of resource/scope-like fields followed by per-row fields. With {@code deferred} the shared fields
     * are declared in the block's first row and written once for the whole block after its last row, the way the streaming builder
     * does when it does not know the row count in advance; otherwise they are written in every row. Both must produce the same batch.
     */
    private static EscfBatch write(boolean deferred, long seed, boolean dictionaryStrings) {
        Random random = new Random(seed);
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE, dictionaryStrings)) {
            int blocks = 1 + random.nextInt(5);
            for (int b = 0; b < blocks; b++) {
                int rows = 1 + random.nextInt(20);
                String service = "service-" + random.nextInt(3);
                boolean hasHost = random.nextBoolean();
                long port = random.nextInt(5);
                double ratio = random.nextInt(3) / 2.0;
                boolean flag = random.nextBoolean();
                // sometimes the shared long column of one block becomes a string in another one, forcing a union column
                boolean portAsString = random.nextInt(4) == 0;
                int serviceField = -1, hostField = -1, portField = -1, ratioField = -1, flagField = -1;
                int firstRow = builder.docCount();
                for (int r = 0; r < rows; r++) {
                    builder.beginRow();
                    builder.longField("@timestamp", 1_000L * b + r);
                    if (deferred && r == 0) {
                        // declared where the per-row variant first writes them, so that both schemas list the leaves in the same order
                        builder.startObject("resource");
                        builder.startObject("attributes");
                        serviceField = builder.declareField("service.name");
                        if (hasHost) {
                            hostField = declareByBytes(builder, "host.name");
                        }
                        portField = declareByBytes(builder, "port");
                        ratioField = builder.declareField("ratio");
                        flagField = builder.declareField("flag");
                        builder.endObject();
                        builder.endObject();
                    }
                    if (deferred == false) {
                        builder.startObject("resource");
                        builder.startObject("attributes");
                        write(builder, "service.name", service);
                        if (hasHost) {
                            write(builder, "host.name", "host-" + b);
                        }
                        if (portAsString) {
                            write(builder, "port", "p" + port);
                        } else {
                            builder.longField("port", port);
                        }
                        builder.doubleField("ratio", ratio);
                        builder.booleanField("flag", flag);
                        builder.endObject();
                        builder.endObject();
                    }
                    if (random.nextBoolean()) {
                        builder.startObject("attributes");
                        builder.stringField("per.row", utf8("v" + random.nextInt(4)), 0, 2);
                        builder.endObject();
                    }
                    builder.startObject("metrics");
                    builder.longField("metric" + random.nextInt(6), random.nextLong());
                    builder.endObject();
                    builder.finishRow();
                }
                if (deferred) {
                    writeRepeated(builder, serviceField, firstRow, rows, service);
                    if (hasHost) {
                        writeRepeated(builder, hostField, firstRow, rows, "host-" + b);
                    }
                    if (portAsString) {
                        writeRepeated(builder, portField, firstRow, rows, "p" + port);
                    } else {
                        builder.repeatedLongField(portField, firstRow, rows, port);
                    }
                    builder.repeatedDoubleField(ratioField, firstRow, rows, ratio);
                    builder.repeatedBooleanField(flagField, firstRow, rows, flag);
                }
            }
            return builder.build();
        }
    }

    private static int declareByBytes(OtlpMetricsBatchBuilder builder, String name) {
        // surround the name with garbage to exercise the offset/length handling of the lookup by bytes
        byte[] bytes = utf8("xx" + name + "yy");
        return builder.declareField(bytes, 2, bytes.length - 4);
    }

    private static void write(OtlpMetricsBatchBuilder builder, String name, String value) {
        byte[] bytes = utf8(value);
        builder.stringField(name, bytes, 0, bytes.length);
    }

    private static void writeRepeated(OtlpMetricsBatchBuilder builder, int field, int firstRow, int rows, String value) {
        byte[] bytes = utf8(value);
        builder.repeatedStringField(field, firstRow, rows, bytes, 0, bytes.length);
    }

    public void testDeferredRepeatedFieldsEqualPerRowFields() {
        for (boolean dictionaryStrings : new boolean[] { true, false }) {
            for (int i = 0; i < 50; i++) {
                long seed = randomLong();
                try (EscfBatch deferred = write(true, seed, dictionaryStrings); EscfBatch rows = write(false, seed, dictionaryStrings)) {
                    assertEquals(rows.docCount(), deferred.docCount());
                    BytesReference expected = rows.data();
                    assertEquals("seed " + seed, expected, deferred.data());
                }
            }
        }
    }

    public void testDeclareFieldByBytesFindsExistingLeaf() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            int byName = builder.declareField("host.name");
            assertEquals(byName, declareByBytes(builder, "host.name"));
            assertEquals(byName, builder.declareField("host.name"));
            builder.startObject("resource");
            int nested = declareByBytes(builder, "host.name");
            assertNotEquals("same name under another parent is another leaf", byName, nested);
            assertEquals(nested, builder.declareField("host.name"));
            builder.endObject();
            assertEquals(byName, declareByBytes(builder, "host.name"));
        }
    }

    public void testDuplicateFieldInRowIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            builder.beginRow();
            builder.longField("a", 1);
            expectThrows(IllegalArgumentException.class, () -> builder.longField("a", 2));
        }
    }

    public void testRepeatedFieldOverWrittenRowsIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            int a = builder.declareField("a");
            builder.beginRow();
            builder.longField(a, 1);
            builder.finishRow();
            builder.beginRow();
            builder.finishRow();
            expectThrows(IllegalArgumentException.class, () -> builder.repeatedLongField(a, 0, 2, 2));
            // rows after the written one are fine
            builder.repeatedLongField(a, 1, 1, 2);
            expectThrows(IllegalArgumentException.class, () -> builder.repeatedLongField(a, 1, 1, 3));
        }
    }

    public void testRepeatedFieldBeyondBatchIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            int a = builder.declareField("a");
            builder.beginRow();
            builder.finishRow();
            expectThrows(IllegalArgumentException.class, () -> builder.repeatedLongField(a, 0, 3, 1));
            expectThrows(IllegalArgumentException.class, () -> builder.repeatedLongField(a, 0, 0, 1));
        }
    }
}
