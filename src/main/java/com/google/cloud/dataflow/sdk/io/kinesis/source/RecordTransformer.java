package com.google.cloud.dataflow.sdk.io.kinesis.source;

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.SingleShardCheckpoint;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import java.util.List;

/**
 * Created by ppastuszka on 11.03.16.
 */
public class RecordTransformer {
    private final RecordsDeaggregator recordsDeaggregator;

    public RecordTransformer() {
        this(new RecordsDeaggregator());
    }

    public RecordTransformer(RecordsDeaggregator recordsDeaggregator) {
        this.recordsDeaggregator = recordsDeaggregator;
    }

    public List<? extends Record> transform(List<Record> records, SingleShardCheckpoint checkpoint) {
        List<UserRecord> deaggregatedRecords = recordsDeaggregator.deaggregate(records);

        List<UserRecord> filteredRecords = Lists.newArrayList();
        for (UserRecord record : deaggregatedRecords) {
            if (checkpoint.isBefore(extendedSequenceNumberFor(record))) {
                filteredRecords.add(record);
            }
        }
        return filteredRecords;
    }

    private ExtendedSequenceNumber extendedSequenceNumberFor(UserRecord record) {
        return new ExtendedSequenceNumber(record.getSequenceNumber(), record.getSubSequenceNumber
                ());
    }
}
