/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

/**
 * Helpers for targeting the exemplar component of a metrics data stream.
 */
public final class ExemplarIndexTarget {

    public static final String SELECTOR = "exemplars";

    private ExemplarIndexTarget() {}

    /**
     * Returns the bulk index target for exemplar documents belonging to the given metrics data stream.
     */
    public static String forDataStream(String dataStreamName) {
        return dataStreamName + "::" + SELECTOR;
    }
}
