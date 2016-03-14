package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.KinesisRecord;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;

import java.util.List;

/**
 * Created by ppastuszka on 11.03.16.
 */
class RecordFilter {
    public List<KinesisRecord> transform(List<KinesisRecord> records,
                                         ShardCheckpoint checkpoint) {
        List<KinesisRecord> filteredRecords = newArrayList();
        for (KinesisRecord record : records) {
            if (checkpoint.isBeforeOrAt(record.getExtendedSequenceNumber())) {
                filteredRecords.add(record);
            }
        }
        return filteredRecords;
    }
}
