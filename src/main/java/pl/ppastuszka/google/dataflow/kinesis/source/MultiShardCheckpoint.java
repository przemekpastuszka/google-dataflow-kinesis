package pl.ppastuszka.google.dataflow.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/***
 *
 */
public class MultiShardCheckpoint extends ArrayList<SingleShardCheckpoint> implements
        UnboundedSource.CheckpointMark {

    public MultiShardCheckpoint() {
        super();
    }

    public MultiShardCheckpoint(List<SingleShardCheckpoint> singleShardCheckpoint) {
        super(singleShardCheckpoint);
    }

    public MultiShardCheckpoint(Iterable<ShardRecordsIterator> iterators) {
        this(newArrayList(Iterables.transform(iterators, new Function<ShardRecordsIterator,
                SingleShardCheckpoint>() {
            @Override
            public SingleShardCheckpoint apply(ShardRecordsIterator shardRecordsIterator) {
                return shardRecordsIterator.getCheckpoint();
            }
        })));
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }
}
