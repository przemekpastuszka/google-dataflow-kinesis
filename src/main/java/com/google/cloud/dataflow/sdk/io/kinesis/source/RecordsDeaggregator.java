package com.google.cloud.dataflow.sdk.io.kinesis.source;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import java.util.List;

/**
 * Created by ppastuszka on 11.03.16.
 */
public class RecordsDeaggregator {
    public List<UserRecord> deaggregate(List<Record> records) {
        return UserRecord.deaggregate(records);
    }
}
