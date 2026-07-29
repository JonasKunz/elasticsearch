/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class WriteIndexComponentSelectorResolverTests extends ESTestCase {

    public void testResolvesExemplarSelectorOnIndexRequest() {
        IndexRequest request = new IndexRequest("metrics-otel::exemplars");
        WriteIndexComponentSelectorResolver.resolve(request);
        assertThat(request.index(), equalTo("metrics-otel"));
        assertThat(request.isWriteToExemplarStore(), is(true));
        assertThat(request.isWriteToFailureStore(), is(false));
    }

    public void testRejectsFailureSelectorOnWrite() {
        IndexRequest request = new IndexRequest("logs::failures");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WriteIndexComponentSelectorResolver.resolve(request)
        );
        assertThat(e.getMessage(), containsString("index component selector [::failures] is not supported for write requests"));
        assertThat(request.index(), equalTo("logs::failures"));
        assertThat(request.isWriteToFailureStore(), is(false));
        assertThat(request.isWriteToExemplarStore(), is(false));
    }

    public void testRejectsDataSelectorOnWrite() {
        IndexRequest request = new IndexRequest("logs::data");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WriteIndexComponentSelectorResolver.resolve(request)
        );
        assertThat(e.getMessage(), containsString("index component selector [::data] is not supported for write requests"));
    }

    public void testNoOpWithoutSelector() {
        IndexRequest request = new IndexRequest("logs");
        WriteIndexComponentSelectorResolver.resolve(request);
        assertThat(request.index(), equalTo("logs"));
        assertThat(request.isWriteToExemplarStore(), is(false));
        assertThat(request.isWriteToFailureStore(), is(false));
    }

    public void testRejectsSelectorOnNonIndexRequest() {
        DeleteRequest request = new DeleteRequest("logs::exemplars", "1");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> WriteIndexComponentSelectorResolver.resolve(request)
        );
        assertThat(e.getMessage(), containsString("index component selector [::exemplars] is only supported on index requests"));
        assertThat(request.index(), equalTo("logs::exemplars"));
    }
}
