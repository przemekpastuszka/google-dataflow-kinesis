import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import utils.TestUtils;

/***
 *
 */
public class CorrectnessIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectnessIntegrationTest.class);

    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    private List<String> testData;

    @Before
    public void setUp() {
        testData = TestUtils.randomStrings(50000);
    }

//    @Test
//    public void readerTestWithKinesisProducer() throws Exception {
//        Future<?> future = startTestPipeline();
//        TestUtils.putRecordsWithKinesisProducer(testData);
//        LOG.info("All data sent to kinesis");
//        future.get(25, TimeUnit.SECONDS);
//    }

    @Test
    public void readerTestWithOldStylePuts() throws Exception {
        Future<?> future = startTestPipeline();
        TestUtils.putRecordsOldStyle(testData);
        LOG.info("All data sent to kinesis");
        future.get(25, TimeUnit.SECONDS);
    }

    private Future<?> startTestPipeline() throws InterruptedException {
        final Pipeline p = TestPipeline.create();
        PCollection<String> result = p.
                apply(Read.
                        from(TestUtils.getTestKinesisSource()).
                        withMaxNumRecords(testData.size())
                ).
                apply(ParDo.of(new TestUtils.ByteArrayToString()));
        DataflowAssert.that(result).containsInAnyOrder(testData);

        Future<?> future = singleThreadExecutor.submit(new Runnable() {
            @Override
            public void run() {
                p.run();
            }
        });
        Thread.sleep(5000);
        return future;
    }
}
