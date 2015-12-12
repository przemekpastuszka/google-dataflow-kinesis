import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;

import static org.joda.time.Duration.standardSeconds;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class CorrectnessE2ETest {
    @Before
    public void setUp() throws IOException {
        TestUtils.deleteTableIfExists(TestUtils.getTestTableReference());
    }

    @After
    public void tearDown() throws IOException {
        TestUtils.deleteTableIfExists(TestUtils.getTestTableReference());
    }

    @Test
    public void testCorrectnessOnDataflowService() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(TestUtils.getTestProject());
        options.setStreaming(true);
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(TestUtils.getTestStagingLocation());
        options.setTempLocation(TestUtils.getTestTempLocation());
        Pipeline p = Pipeline.create(options);

        List<String> testData = TestUtils.randomStrings(2000);

        p.
                apply(Read.from(TestUtils.getTestKinesisSource())).
                apply(Window.<byte[]>into(FixedWindows.of(standardSeconds(10)))).
                apply(ParDo.of(new TestUtils.ByteArrayToString())).
                apply(ParDo.of(new TestUtils.ToTableRow())).
                apply(BigQueryIO.Write.
                        to(TestUtils.getTestTableReference()).
                        withSchema(TestUtils.getTestTableSchema()));

        TestUtils.putRecords(testData);

    }


}
