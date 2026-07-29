/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

/**
 * Resolves {@link IndexComponentSelector} syntax on write requests after authorization has observed the raw target expression.
 * Selectors are stripped from {@link IndexRequest#index()} and translated into a transient exemplar-store routing flag
 * when {@code ::exemplars} is used. Other selectors are rejected for client writes.
 */
public final class WriteIndexComponentSelectorResolver {

    private WriteIndexComponentSelectorResolver() {}

    /**
     * Parses an optional selector suffix on the provided write request. When {@code ::exemplars} is present, the request index is
     * rewritten to the parent resource name and a transient exemplar-store routing flag is set on {@link IndexRequest} instances.
     *
     * @throws IllegalArgumentException if the selector is unsupported for writes or present on a non-index request
     */
    public static void resolve(DocWriteRequest<?> request) {
        var expressionAndSelector = IndexNameExpressionResolver.splitSelectorExpression(request.index());
        String selectorString = expressionAndSelector.v2();
        if (selectorString == null) {
            return;
        }
        if (request instanceof IndexRequest indexRequest) {
            IndexComponentSelector selector = IndexComponentSelector.getByKeyOrThrow(selectorString);
            switch (selector) {
                case DATA -> throw new IllegalArgumentException(
                    "index component selector [::data] is not supported for write requests; omit the selector to write to backing indices"
                );
                case FAILURES -> throw new IllegalArgumentException(
                    "index component selector [::failures] is not supported for write requests"
                );
                case EXEMPLARS -> indexRequest.setWriteToExemplarStore(true).index(expressionAndSelector.v1());
            }
            return;
        }
        throw new IllegalArgumentException("index component selector [::" + selectorString + "] is only supported on index requests");
    }
}
