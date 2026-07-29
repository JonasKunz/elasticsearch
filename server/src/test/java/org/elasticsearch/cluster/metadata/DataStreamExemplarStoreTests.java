/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamExemplarStoreTests extends AbstractXContentSerializingTestCase<DataStreamExemplarStore> {

    @Override
    protected Writeable.Reader<DataStreamExemplarStore> instanceReader() {
        return DataStreamExemplarStore::new;
    }

    @Override
    protected DataStreamExemplarStore createTestInstance() {
        return randomExemplarStore();
    }

    @Override
    protected DataStreamExemplarStore mutateInstance(DataStreamExemplarStore instance) {
        var enabled = instance.enabled();
        var indexTemplate = instance.indexTemplate();
        switch (randomIntBetween(0, 1)) {
            case 0 -> enabled = enabled != null && indexTemplate != null && randomBoolean() ? null : Boolean.FALSE.equals(enabled);
            case 1 -> indexTemplate = indexTemplate != null && enabled != null && randomBoolean()
                ? null
                : randomValueOtherThan(indexTemplate, () -> randomAlphaOfLength(10));
            default -> throw new IllegalArgumentException("illegal randomisation branch");
        }
        if (Boolean.TRUE.equals(enabled) && (indexTemplate == null || indexTemplate.isBlank())) {
            indexTemplate = randomAlphaOfLength(10);
        }
        return new DataStreamExemplarStore(enabled, indexTemplate);
    }

    @Override
    protected DataStreamExemplarStore doParseInstance(XContentParser parser) throws IOException {
        return DataStreamExemplarStore.fromXContent(parser);
    }

    static DataStreamExemplarStore randomExemplarStore() {
        boolean enabledDefined = randomBoolean();
        boolean indexTemplateDefined = enabledDefined == false || randomBoolean();
        Boolean enabled = enabledDefined ? randomBoolean() : null;
        String indexTemplate = indexTemplateDefined ? randomAlphaOfLength(10) : null;
        if (Boolean.TRUE.equals(enabled) && indexTemplate == null) {
            indexTemplate = randomAlphaOfLength(10);
        }
        return new DataStreamExemplarStore(enabled, indexTemplate);
    }

    public void testInvalidEmptyConfiguration() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamExemplarStore(null, null));
        assertThat(exception.getMessage(), containsString("at least one non-null configuration value"));
    }

    public void testEnabledRequiresIndexTemplate() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamExemplarStore(true, null));
        assertThat(exception.getMessage(), containsString("index_template"));
        exception = expectThrows(IllegalArgumentException.class, () -> new DataStreamExemplarStore(true, "   "));
        assertThat(exception.getMessage(), containsString("index_template"));
    }

    public void testPartialTemplateComposable() {
        assertThat(new DataStreamExemplarStore.Template(true, null).enabled().get(), equalTo(true));
        assertThat(new DataStreamExemplarStore.Template(null, "metrics-exemplars").indexTemplate().get(), equalTo("metrics-exemplars"));

        DataStreamExemplarStore.Builder builder = DataStreamExemplarStore.builder()
            .composeTemplate(new DataStreamExemplarStore.Template(true, null));
        builder.composeTemplate(new DataStreamExemplarStore.Template(null, "metrics-exemplars"));
        assertThat(builder.build(), equalTo(new DataStreamExemplarStore(true, "metrics-exemplars")));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DataStreamExemplarStore.builder().enabled(true).build()
        );
        assertThat(exception.getMessage(), containsString("index_template"));
    }

    public void testTemplateComposition() {
        DataStreamExemplarStore.Builder builder = DataStreamExemplarStore.builder().enabled(true).indexTemplate("metrics-exemplars");
        assertThat(builder.build().indexTemplate(), equalTo("metrics-exemplars"));

        builder.composeTemplate(new DataStreamExemplarStore.Template(ResettableValue.reset(), ResettableValue.undefined()));
        assertThat(builder.build(), equalTo(new DataStreamExemplarStore(null, "metrics-exemplars")));

        builder = DataStreamExemplarStore.builder().indexTemplate("first-template");
        builder.composeTemplate(
            new DataStreamExemplarStore.Template(ResettableValue.undefined(), ResettableValue.create("second-template"))
        );
        assertThat(builder.build().indexTemplate(), equalTo("second-template"));
    }
}
