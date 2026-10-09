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
     * Writes {@code blocks} blocks of resource/scope-like fields followed by per-row fields. With {@code useBlocks} the shared fields
     * are written once per block, otherwise in every row; both must produce the same batch.
     */
    private static EscfBatch write(boolean useBlocks, long seed) {
        Random random = new Random(seed);
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
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
                if (useBlocks) {
                    builder.beginBlock(rows);
                }
                for (int r = 0; r < rows; r++) {
                    builder.beginRow();
                    builder.longField("@timestamp", 1_000L * b + r);
                    if (useBlocks == false || r == 0) {
                        builder.startObject("resource");
                        builder.startObject("attributes");
                        write(builder, useBlocks, "service.name", service);
                        if (hasHost) {
                            write(builder, useBlocks, "host.name", "host-" + b);
                        }
                        if (portAsString) {
                            write(builder, useBlocks, "port", "p" + port);
                        } else {
                            write(builder, useBlocks, "port", port);
                        }
                        write(builder, useBlocks, "ratio", ratio);
                        write(builder, useBlocks, "flag", flag);
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
            }
            return builder.build();
        }
    }

    private static void write(OtlpMetricsBatchBuilder builder, boolean block, String name, String value) {
        byte[] bytes = utf8(value);
        if (block) {
            builder.blockStringField(name, bytes, 0, bytes.length);
        } else {
            builder.stringField(name, bytes, 0, bytes.length);
        }
    }

    private static void write(OtlpMetricsBatchBuilder builder, boolean block, String name, long value) {
        if (block) {
            builder.blockLongField(name, value);
        } else {
            builder.longField(name, value);
        }
    }

    private static void write(OtlpMetricsBatchBuilder builder, boolean block, String name, double value) {
        if (block) {
            builder.blockDoubleField(name, value);
        } else {
            builder.doubleField(name, value);
        }
    }

    private static void write(OtlpMetricsBatchBuilder builder, boolean block, String name, boolean value) {
        if (block) {
            builder.blockBooleanField(name, value);
        } else {
            builder.booleanField(name, value);
        }
    }

    public void testBlockFieldsEqualPerRowFields() {
        for (int i = 0; i < 50; i++) {
            long seed = randomLong();
            try (EscfBatch blocks = write(true, seed); EscfBatch rows = write(false, seed)) {
                assertEquals(rows.docCount(), blocks.docCount());
                BytesReference expected = rows.data();
                assertEquals("seed " + seed, expected, blocks.data());
            }
        }
    }

    public void testDuplicateFieldInRowIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            builder.beginRow();
            builder.longField("a", 1);
            expectThrows(IllegalArgumentException.class, () -> builder.longField("a", 2));
        }
    }

    public void testDuplicateBlockFieldIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            builder.beginBlock(3);
            builder.beginRow();
            builder.blockLongField("a", 1);
            expectThrows(IllegalArgumentException.class, () -> builder.blockLongField("a", 2));
        }
    }

    public void testBlockFieldOutsideFirstRowIsRejected() {
        try (OtlpMetricsBatchBuilder builder = new OtlpMetricsBatchBuilder()) {
            builder.beginBlock(2);
            builder.beginRow();
            builder.finishRow();
            builder.beginRow();
            expectThrows(IllegalStateException.class, () -> builder.blockLongField("a", 1));
        }
    }
}
