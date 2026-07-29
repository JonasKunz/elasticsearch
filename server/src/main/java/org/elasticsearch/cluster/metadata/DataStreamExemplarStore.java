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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Holds data stream exemplar store metadata that enables or disables exemplar storage and references the index template
 * that defines exemplar index mappings and settings.
 */
public record DataStreamExemplarStore(@Nullable Boolean enabled, @Nullable String indexTemplate)
    implements
        SimpleDiffable<DataStreamExemplarStore>,
        ToXContentObject {

    public static final String EXEMPLAR_STORE = "exemplar_store";
    public static final String ENABLED = "enabled";
    public static final String INDEX_TEMPLATE = "index_template";
    private static final String EMPTY_EXEMPLAR_STORE_ERROR_MESSAGE =
        "Exemplar store configuration should have at least one non-null configuration value.";
    private static final String ENABLED_REQUIRES_INDEX_TEMPLATE_ERROR_MESSAGE =
        "Exemplar store requires [index_template] when [enabled] is true.";

    public static final ParseField ENABLED_FIELD = new ParseField(ENABLED);
    public static final ParseField INDEX_TEMPLATE_FIELD = new ParseField(INDEX_TEMPLATE);

    public static final ConstructingObjectParser<DataStreamExemplarStore, Void> PARSER = new ConstructingObjectParser<>(
        EXEMPLAR_STORE,
        false,
        (args, unused) -> new DataStreamExemplarStore((Boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ENABLED_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_TEMPLATE_FIELD);
    }

    private static final TransportVersion INTRODUCE_EXEMPLAR_STORE = TransportVersion.fromName("introduce_exemplar_store");

    /**
     * @param enabled true when the exemplar store is enabled, false when disabled, null when unset
     * @param indexTemplate name of the composable index template for exemplar indices
     * @throws IllegalArgumentException when all constructor arguments are null, or when enabled is true without a non-blank index template
     */
    public DataStreamExemplarStore {
        if (enabled == null && indexTemplate == null) {
            throw new IllegalArgumentException(EMPTY_EXEMPLAR_STORE_ERROR_MESSAGE);
        }
        validateEnabledRequiresIndexTemplate(enabled, indexTemplate);
    }

    public DataStreamExemplarStore(StreamInput in) throws IOException {
        this(in.readOptionalBoolean(), in.getTransportVersion().supports(INTRODUCE_EXEMPLAR_STORE) ? in.readOptionalString() : null);
    }

    public static Diff<DataStreamExemplarStore> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamExemplarStore::new, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(enabled);
        if (out.getTransportVersion().supports(INTRODUCE_EXEMPLAR_STORE)) {
            out.writeOptionalString(indexTemplate);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (enabled != null) {
            builder.field(ENABLED_FIELD.getPreferredName(), enabled);
        }
        if (indexTemplate != null) {
            builder.field(INDEX_TEMPLATE_FIELD.getPreferredName(), indexTemplate);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamExemplarStore fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * This class is only used in template configuration. It wraps the fields of {@link DataStreamExemplarStore} with
     * {@link ResettableValue} to allow a user to signal when they want to reset any previously encountered values during template
     * composition.
     */
    public record Template(ResettableValue<Boolean> enabled, ResettableValue<String> indexTemplate) implements Writeable, ToXContentObject {

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Template, Void> PARSER = new ConstructingObjectParser<>(
            "exemplar_store_template",
            false,
            (args, unused) -> new Template(
                args[0] == null ? ResettableValue.undefined() : (ResettableValue<Boolean>) args[0],
                args[1] == null ? ResettableValue.undefined() : (ResettableValue<String>) args[1]
            )
        );

        static {
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL
                    ? ResettableValue.reset()
                    : ResettableValue.create(p.booleanValue()),
                ENABLED_FIELD,
                ObjectParser.ValueType.BOOLEAN_OR_NULL
            );
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> p.currentToken() == XContentParser.Token.VALUE_NULL ? ResettableValue.reset() : ResettableValue.create(p.text()),
                INDEX_TEMPLATE_FIELD,
                ObjectParser.ValueType.STRING_OR_NULL
            );
        }

        public Template(@Nullable Boolean enabled, @Nullable String indexTemplate) {
            this(ResettableValue.create(enabled), ResettableValue.create(indexTemplate));
        }

        public Template {
            if (enabled.isDefined() == false && indexTemplate.isDefined() == false) {
                throw new IllegalArgumentException(EMPTY_EXEMPLAR_STORE_ERROR_MESSAGE);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ResettableValue.write(out, enabled, StreamOutput::writeBoolean);
            if (out.getTransportVersion().supports(INTRODUCE_EXEMPLAR_STORE)) {
                ResettableValue.write(out, indexTemplate, StreamOutput::writeString);
            }
        }

        public static Template read(StreamInput in) throws IOException {
            ResettableValue<Boolean> enabled = ResettableValue.read(in, StreamInput::readBoolean);
            ResettableValue<String> indexTemplate = ResettableValue.undefined();
            if (in.getTransportVersion().supports(INTRODUCE_EXEMPLAR_STORE)) {
                indexTemplate = ResettableValue.read(in, StreamInput::readString);
            }
            return new Template(enabled, indexTemplate);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            enabled.toXContent(builder, params, ENABLED_FIELD.getPreferredName());
            indexTemplate.toXContent(builder, params, INDEX_TEMPLATE_FIELD.getPreferredName());
            builder.endObject();
            return builder;
        }

        public static Template fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(Template template) {
        return new Builder(template);
    }

    public static Builder builder(DataStreamExemplarStore exemplarStore) {
        return new Builder(exemplarStore);
    }

    /**
     * Builder that is able to create either a {@link DataStreamExemplarStore} or its respective {@link Template}, and compose templates
     * during index template composition.
     */
    public static class Builder {
        private Boolean enabled = null;
        private String indexTemplate = null;

        private Builder() {}

        private Builder(Template template) {
            if (template != null) {
                enabled = template.enabled.get();
                indexTemplate = template.indexTemplate.get();
            }
        }

        private Builder(DataStreamExemplarStore exemplarStore) {
            if (exemplarStore != null) {
                enabled = exemplarStore.enabled;
                indexTemplate = exemplarStore.indexTemplate;
            }
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder enabled(ResettableValue<Boolean> enabled) {
            if (enabled.shouldReset()) {
                this.enabled = null;
            } else if (enabled.isDefined()) {
                this.enabled = enabled.get();
            }
            return this;
        }

        public Builder indexTemplate(String indexTemplate) {
            this.indexTemplate = indexTemplate;
            return this;
        }

        public Builder indexTemplate(ResettableValue<String> indexTemplate) {
            if (indexTemplate.shouldReset()) {
                this.indexTemplate = null;
            } else if (indexTemplate.isDefined()) {
                this.indexTemplate = indexTemplate.get();
            }
            return this;
        }

        public Builder composeTemplate(DataStreamExemplarStore.Template exemplarStore) {
            this.enabled(exemplarStore.enabled());
            this.indexTemplate(exemplarStore.indexTemplate());
            return this;
        }

        @Nullable
        public DataStreamExemplarStore build() {
            if (enabled == null && indexTemplate == null) {
                return null;
            }
            return new DataStreamExemplarStore(enabled, indexTemplate);
        }

        @Nullable
        public DataStreamExemplarStore.Template buildTemplate() {
            if (enabled == null && indexTemplate == null) {
                return null;
            }
            return new Template(enabled, indexTemplate);
        }
    }

    private static void validateEnabledRequiresIndexTemplate(@Nullable Boolean enabled, @Nullable String indexTemplate) {
        if (Boolean.TRUE.equals(enabled) && Strings.hasText(indexTemplate) == false) {
            throw new IllegalArgumentException(ENABLED_REQUIRES_INDEX_TEMPLATE_ERROR_MESSAGE);
        }
    }
}
