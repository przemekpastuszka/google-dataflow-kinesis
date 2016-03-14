package com.google.cloud.dataflow.sdk.io.kinesis.client.response;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

/**
 * Created by ppastuszka on 14.03.16.
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

    public byte[] getId() {
        return getExtendedSequenceNumber().toString().getBytes(Charsets.UTF_8);
    }
}
