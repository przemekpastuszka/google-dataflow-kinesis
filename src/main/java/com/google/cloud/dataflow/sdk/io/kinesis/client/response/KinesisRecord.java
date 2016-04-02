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
package com.google.cloud.dataflow.sdk.io.kinesis.client.response;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

/**
 * {@link UserRecord} enhanced with utility methods.
 */
public class KinesisRecord extends UserRecord {
    public KinesisRecord(UserRecord record) {
        super(record.isAggregated(),
                record,
                record.getSubSequenceNumber(),
                record.getExplicitHashKey());
    }

    public ExtendedSequenceNumber getExtendedSequenceNumber() {
        return new ExtendedSequenceNumber(getSequenceNumber(), getSubSequenceNumber());
    }

    /***
     * @return unique id of the record based on its position in the stream
     */
    public byte[] getUniqueId() {
        return getExtendedSequenceNumber().toString().getBytes(Charsets.UTF_8);
    }
}
