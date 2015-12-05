import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import org.junit.Test;
import pl.ppastuszka.google.dataflow.kinesis.client.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

import java.nio.ByteBuffer;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class CorrectnessTest {
    @Test
    public void readerTest() throws Exception {
        String streamName = System.getenv("TEST_KINESIS_STREAM");
        AmazonKinesis kinesis = new SimpleKinesisClientProvider().getKinesisClient();
        kinesis.putRecord(streamName, ByteBuffer.wrap("aaa".getBytes("UTF-8")), "0");

        Pipeline p = TestPipeline.create();
        KinesisDataflowSource source = new KinesisDataflowSource(new SimpleKinesisClientProvider(), streamName, ShardIteratorType.TRIM_HORIZON);

        p.apply(Read.named("kinesis reader").from(source).withMaxNumRecords(5)).apply(ParDo.of(new byteArrayToString())).apply
                (TextIO.Write.to("/home/ppastuszka/data"));
        p.run();
    }

    private static class byteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            c.output(new String(c.element(), "UTF-8"));
        }
    }
}
