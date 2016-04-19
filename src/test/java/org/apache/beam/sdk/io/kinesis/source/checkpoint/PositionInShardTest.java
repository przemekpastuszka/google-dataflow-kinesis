/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kinesis.source.checkpoint;

import org.apache.beam.sdk.io.kinesis.client.SimplifiedKinesisClient;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
        .LATEST;
import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
        .TRIM_HORIZON;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;

/**
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class PositionInShardTest {
    public static final String AT_SEQUENCE_SHARD_IT = "AT_SEQUENCE_SHARD_IT";
    public static final String AFTER_SEQUENCE_SHARD_IT = "AFTER_SEQUENCE_SHARD_IT";
    private static final String STREAM_NAME = "STREAM";
    private static final String SHARD_ID = "SHARD_ID";
    @Mock
    private SimplifiedKinesisClient client;

    @Before
    public void setUp() throws IOException {
        when(client.getShardIterator(
                eq(STREAM_NAME), eq(SHARD_ID), eq(AT_SEQUENCE_NUMBER), anyString())).
                thenReturn(AT_SEQUENCE_SHARD_IT);
        when(client.getShardIterator(
                eq(STREAM_NAME), eq(SHARD_ID), eq(AFTER_SEQUENCE_NUMBER), anyString())).
                thenReturn(AFTER_SEQUENCE_SHARD_IT);
    }

    @Test
    public void testProvidingShardIterator() throws IOException {
        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", null).obtainShardIterator(client))
                .isEqualTo(AT_SEQUENCE_SHARD_IT);
        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", null).obtainShardIterator(client))
                .isEqualTo(AFTER_SEQUENCE_SHARD_IT);
        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 10L).obtainShardIterator(client)).isEqualTo
                (AT_SEQUENCE_SHARD_IT);
        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 10L).obtainShardIterator(client))
                .isEqualTo(AT_SEQUENCE_SHARD_IT);
    }

    @Test
    public void testComparisonWithExtendedSequenceNumber() {
        assertThat(new PositionInShard("", "", LATEST).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(new PositionInShard("", "", TRIM_HORIZON).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "10", 1L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isFalse();

        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 1L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isFalse();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("99", 1L)
        )).isFalse();
    }

    private PositionInShard checkpoint(ShardIteratorType iteratorType, String sequenceNumber,
                                       Long subSequenceNumber) {
        return new PositionInShard(STREAM_NAME, SHARD_ID, iteratorType, sequenceNumber,
                subSequenceNumber);
    }
}
