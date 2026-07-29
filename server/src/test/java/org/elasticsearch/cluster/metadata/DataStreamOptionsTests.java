/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamOptionsTests extends AbstractXContentSerializingTestCase<DataStreamOptions> {

    private static final TransportVersion SETTINGS_IN_DATA_STREAMS = TransportVersion.fromName("settings_in_data_streams");

    @Override
    protected Writeable.Reader<DataStreamOptions> instanceReader() {
        return DataStreamOptions::read;
    }

    @Override
    protected DataStreamOptions createTestInstance() {
        return randomDataStreamOptions();
    }

    public static DataStreamOptions randomDataStreamOptions() {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> DataStreamOptions.EMPTY;
            case 1 -> DataStreamOptions.FAILURE_STORE_DISABLED;
            case 2 -> DataStreamOptions.FAILURE_STORE_ENABLED;
            case 3 -> new DataStreamOptions(null, DataStreamExemplarStoreTests.randomExemplarStore());
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        };
    }

    @Override
    protected DataStreamOptions mutateInstance(DataStreamOptions instance) throws IOException {
        var failureStore = instance.failureStore();
        var exemplarStore = instance.exemplarStore();
        if (failureStore == null && exemplarStore == null) {
            return randomBoolean()
                ? new DataStreamOptions(DataStreamFailureStoreTests.randomFailureStore(), null)
                : new DataStreamOptions(null, DataStreamExemplarStoreTests.randomExemplarStore());
        }
        switch (randomIntBetween(0, 2)) {
            case 0 -> failureStore = failureStore == null ? DataStreamFailureStoreTests.randomFailureStore()
                : randomBoolean() ? null
                : randomValueOtherThan(failureStore, DataStreamFailureStoreTests::randomFailureStore);
            case 1 -> exemplarStore = exemplarStore == null ? DataStreamExemplarStoreTests.randomExemplarStore()
                : randomBoolean() ? null
                : randomValueOtherThan(exemplarStore, DataStreamExemplarStoreTests::randomExemplarStore);
            case 2 -> {
                failureStore = DataStreamFailureStoreTests.randomFailureStore();
                exemplarStore = DataStreamExemplarStoreTests.randomExemplarStore();
            }
            default -> throw new IllegalArgumentException("Illegal randomisation branch");
        }
        return new DataStreamOptions(failureStore, exemplarStore);
    }

    @Override
    protected DataStreamOptions doParseInstance(XContentParser parser) throws IOException {
        return DataStreamOptions.fromXContent(parser);
    }

    public void testBackwardCompatibility() throws IOException {
        DataStreamOptions result = copyInstance(DataStreamOptions.EMPTY, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.EMPTY));

        DataStreamOptions withEnabled = new DataStreamOptions(
            new DataStreamFailureStore(randomBoolean(), DataStreamLifecycleTests.randomFailuresLifecycle())
        );
        result = copyInstance(withEnabled, SETTINGS_IN_DATA_STREAMS);
        assertThat(result.failureStore().enabled(), equalTo(withEnabled.failureStore().enabled()));
        assertThat(result.failureStore().lifecycle(), nullValue());

        DataStreamOptions withoutEnabled = new DataStreamOptions(
            new DataStreamFailureStore(null, DataStreamLifecycleTests.randomFailuresLifecycle())
        );
        result = copyInstance(withoutEnabled, SETTINGS_IN_DATA_STREAMS);
        assertThat(result, equalTo(DataStreamOptions.EMPTY));

        DataStreamOptions withExemplarStore = new DataStreamOptions(null, new DataStreamExemplarStore(true, "metrics-exemplars-template"));
        result = copyInstance(withExemplarStore, TransportVersion.fromName("introduce_exemplar_store"));
        assertThat(result.exemplarStore().enabled(), equalTo(true));
        assertThat(result.exemplarStore().indexTemplate(), equalTo("metrics-exemplars-template"));

        result = copyInstance(withExemplarStore, SETTINGS_IN_DATA_STREAMS);
        assertThat(result.exemplarStore(), nullValue());
    }

    public void testExemplarStoreTemplateComposition() {
        DataStreamOptions.Template enabledOnly = new DataStreamOptions.Template(null, new DataStreamExemplarStore.Template(true, null));
        DataStreamOptions.Template templateOnly = new DataStreamOptions.Template(
            null,
            new DataStreamExemplarStore.Template(null, "metrics-exemplars")
        );
        DataStreamOptions resolved = DataStreamOptions.builder(enabledOnly).composeTemplate(templateOnly).build();
        assertThat(resolved.exemplarStore(), equalTo(new DataStreamExemplarStore(true, "metrics-exemplars")));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamOptions.builder(enabledOnly).build()
        );
        assertThat(exception.getMessage(), containsString("index_template"));
    }
}
