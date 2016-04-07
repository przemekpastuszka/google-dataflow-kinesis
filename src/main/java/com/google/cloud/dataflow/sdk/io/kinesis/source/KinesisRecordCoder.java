package com.google.cloud.dataflow.sdk.io.kinesis.source;

import com.amazonaws.services.kinesis.model.Record;
import com.google.cloud.dataflow.sdk.coders.*;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by p.pastuszka on 07/04/16.
 */
public class KinesisRecordCoder extends StandardCoder<Record> {
    private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
    private static final ByteArrayCoder byteArrayCoder = ByteArrayCoder.of();
    private static final InstantCoder instantCoder = InstantCoder.of();

    public static KinesisRecordCoder of() {
        return new KinesisRecordCoder();
    }

    @Override
    public void encode(Record value, OutputStream outStream, Context context) throws CoderException, IOException {
        Context nested = context.nested();
        byteArrayCoder.encode(value.getData().array(), outStream, nested);
        stringCoder.encode(value.getSequenceNumber(), outStream, nested);
        stringCoder.encode(value.getPartitionKey(), outStream, nested);
        instantCoder.encode(new Instant(value.getApproximateArrivalTimestamp()), outStream, nested);

    }

    @Override
    public Record decode(InputStream inStream, Context context) throws CoderException, IOException {
        Context nested = context.nested();
        return new Record().
                withData(ByteBuffer.wrap(byteArrayCoder.decode(inStream, nested))).
                withSequenceNumber(stringCoder.decode(inStream, nested)).
                withPartitionKey(stringCoder.decode(inStream, nested)).
                withApproximateArrivalTimestamp(instantCoder.decode(inStream, nested).toDate());
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        stringCoder.verifyDeterministic();
        byteArrayCoder.verifyDeterministic();
        instantCoder.verifyDeterministic();
    }
}
