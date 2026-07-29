/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexComponentSelectorTests extends ESTestCase {

    private static final TransportVersion INTRODUCE_EXEMPLAR_STORE = TransportVersion.fromName("introduce_exemplar_store");
    private static final TransportVersion REMOVE_ALL_APPLICABLE_SELECTOR = TransportVersion.fromName("remove_all_applicable_selector");

    public void testIndexComponentSelectorFromKey() {
        assertThat(IndexComponentSelector.getByKey("data"), equalTo(IndexComponentSelector.DATA));
        assertThat(IndexComponentSelector.getByKey("failures"), equalTo(IndexComponentSelector.FAILURES));
        assertThat(IndexComponentSelector.getByKey("exemplars"), equalTo(IndexComponentSelector.EXEMPLARS));
        assertThat(IndexComponentSelector.getByKey("*"), nullValue());
        assertThat(IndexComponentSelector.getByKey("d*ta"), nullValue());
        assertThat(IndexComponentSelector.getByKey("_all"), nullValue());
        assertThat(IndexComponentSelector.getByKey("**"), nullValue());
        assertThat(IndexComponentSelector.getByKey("failure"), nullValue());
    }

    public void testIndexComponentSelectorFromId() {
        assertThat(IndexComponentSelector.getById((byte) 0), equalTo(IndexComponentSelector.DATA));
        assertThat(IndexComponentSelector.getById((byte) 1), equalTo(IndexComponentSelector.FAILURES));
        assertThat(IndexComponentSelector.getById((byte) 3), equalTo(IndexComponentSelector.EXEMPLARS));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> IndexComponentSelector.getById((byte) 2));
        assertThat(
            exception.getMessage(),
            containsString("Unknown id of index component selector [2], available options are: {0=DATA, 1=FAILURES, 3=EXEMPLARS}")
        );
    }

    public void testExemplarSelectorWireRoundTrip() throws IOException {
        assertWireRoundTrip(IndexComponentSelector.EXEMPLARS, INTRODUCE_EXEMPLAR_STORE);
    }

    public void testDataAndFailureSelectorsWireRoundTripOnLegacyTransportVersion() throws IOException {
        TransportVersion legacyVersion = TransportVersionUtils.getPreviousVersion(REMOVE_ALL_APPLICABLE_SELECTOR);
        assertWireRoundTrip(IndexComponentSelector.DATA, legacyVersion);
        assertWireRoundTrip(IndexComponentSelector.FAILURES, legacyVersion);
    }

    public void testExemplarSelectorWriteFailsOnLegacyTransportVersion() {
        TransportVersion legacyVersion = TransportVersionUtils.getPreviousVersion(INTRODUCE_EXEMPLAR_STORE);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(legacyVersion);
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> IndexComponentSelector.EXEMPLARS.writeTo(out)
            );
            assertThat(exception.getMessage(), containsString("exemplars"));
            assertThat(exception.getMessage(), containsString("unsupported"));
        }
    }

    public void testExemplarSelectorReadFailsOnLegacyTransportVersion() throws IOException {
        TransportVersion legacyVersion = TransportVersionUtils.getPreviousVersion(INTRODUCE_EXEMPLAR_STORE);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(legacyVersion);
            out.writeByte(IndexComponentSelector.EXEMPLARS.getId());
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(legacyVersion);
                IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> IndexComponentSelector.read(in));
                assertThat(exception.getMessage(), containsString("exemplars"));
                assertThat(exception.getMessage(), containsString("unsupported"));
            }
        }
    }

    public void testLegacyIdTwoMapsToDataOnPreRemoveAllApplicableSelectorVersion() throws IOException {
        TransportVersion legacyVersion = TransportVersionUtils.getPreviousVersion(REMOVE_ALL_APPLICABLE_SELECTOR);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(legacyVersion);
            out.writeByte((byte) 2);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(legacyVersion);
                assertThat(IndexComponentSelector.read(in), equalTo(IndexComponentSelector.DATA));
            }
        }
    }

    private static void assertWireRoundTrip(IndexComponentSelector selector, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            selector.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                assertThat(IndexComponentSelector.read(in), equalTo(selector));
            }
        }
    }
}
