package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import java.util.List;

/**
 * Created by ppastuszka on 11.03.16.
 */
public class RecordTransformer {
    public List<UserRecord> transform(List<UserRecord> records, ShardCheckpoint checkpoint) {
        List<UserRecord> filteredRecords = newArrayList();
        for (UserRecord record : records) {
            if (checkpoint.isBeforeOrAt(extendedSequenceNumberFor(record))) {
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
