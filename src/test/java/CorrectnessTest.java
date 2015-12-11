import com.amazonaws.services.kinesis.AmazonKinesis;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import org.junit.Test;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

import java.nio.ByteBuffer;
import java.util.concurrent.*;


public class CorrectnessTest {
    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    @Test
    public void readerTest() throws Exception {
        String streamName = System.getenv("TEST_KINESIS_STREAM");
        AmazonKinesis kinesis = new SimpleKinesisClientProvider().getKinesisClient();


        final Pipeline p = TestPipeline.create();
        KinesisDataflowSource source = TestUtils.getTestKinesisSource();

        p.apply(Read.from(source).withMaxNumRecords(1)).
                apply(ParDo.of(new byteArrayToString())).
                apply(TextIO.Write.to("/home/ppastuszka/data"));
        Future<?> future = singleThreadExecutor.submit(new Runnable() {
            @Override
            public void run() {
                p.run();
            }
        });

        Thread.sleep(1000);
        kinesis.putRecord(streamName, ByteBuffer.wrap("aaa".getBytes("UTF-8")), "0");

        future.get(10, TimeUnit.SECONDS);
    }

    private static class byteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            c.output(new String(c.element(), "UTF-8"));
        }
    }
}
