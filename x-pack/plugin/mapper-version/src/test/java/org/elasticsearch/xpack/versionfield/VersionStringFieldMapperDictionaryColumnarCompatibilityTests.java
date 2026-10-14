/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

/**
 * Runs every scenario of {@link VersionStringFieldMapperColumnarCompatibilityTests} with dictionary-encoded source STRING columns, the
 * layout the OTLP metrics path produces, so the dictionary path of {@link VersionStringFieldMapper#mapColumnBatch} is checked against
 * the row path too.
 */
public class VersionStringFieldMapperDictionaryColumnarCompatibilityTests extends VersionStringFieldMapperColumnarCompatibilityTests {

    @Override
    protected boolean dictionaryStrings() {
        return true;
    }
}
