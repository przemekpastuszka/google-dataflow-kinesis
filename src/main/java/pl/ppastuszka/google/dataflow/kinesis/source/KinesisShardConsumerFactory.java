package pl.ppastuszka.google.dataflow.kinesis.source;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SingleThreadShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;

/**
 * Created by ppastuszka on 21.12.15.
 */
public class KinesisShardConsumerFactory {
    SingleThreadShardConsumer getConsumer(String shardId, IKinesisProxy proxy, ICheckpoint
            checkpoint, IRecordProcessor recordProcessor) {
        return new SingleThreadShardConsumer(shardId, proxy, checkpoint, recordProcessor);
    }
}
