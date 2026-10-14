/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Runs every scenario of {@link NumberFieldMapperColumnarCompatibilityTests} with run-length encoded source columns, the layout the
 * OTLP metrics path produces, so {@link NumberFieldMapper#mapColumnBatch} is checked against the row path on that layout too.
 */
public class NumberFieldMapperRleColumnarCompatibilityTests extends NumberFieldMapperColumnarCompatibilityTests {

    @Override
    protected boolean runLengthEncoded() {
        return true;
    }
}
