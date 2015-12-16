import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
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
    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    @Test
    public void readerTest() throws Exception {
        List<String> testData = TestUtils.randomStrings(2000);

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
        Thread.sleep(4000);
        TestUtils.putRecords(testData);
        future.get(12, TimeUnit.SECONDS);
    }
}
