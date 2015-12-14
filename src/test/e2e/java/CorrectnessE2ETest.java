import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets.newHashSet;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;

import static org.fest.assertions.Assertions.assertThat;
import static org.joda.time.Duration.standardSeconds;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.List;
import utils.BQ;
import utils.TestConfiguration;
import utils.TestUtils;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class CorrectnessE2ETest {
    private TableReference testTable;

    @Before
    public void setUp() throws IOException {
        testTable = TestUtils.getTestTableReference();
        BQ.get().deleteTableIfExists(testTable);
    }

    @After
    public void tearDown() throws IOException {

        BQ.get().deleteTableIfExists(testTable);
    }

    @Test
    public void testCorrectnessOnDataflowService() throws InterruptedException, IOException {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(TestConfiguration.get().getTestProject());
        options.setStreaming(true);
        options.setJobName("e2eKinesisConnectorCorrectness");
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(TestConfiguration.get().getTestStagingLocation());
        options.setTempLocation(TestConfiguration.get().getTestTempLocation());
        Pipeline p = Pipeline.create(options);

        List<String> testData = TestUtils.randomStrings(20000);

        p.
                apply(Read.from(TestUtils.getTestKinesisSource())).
                apply(Window.<byte[]>into(FixedWindows.of(standardSeconds(10)))).
                apply(ParDo.of(new TestUtils.ByteArrayToString())).
                apply(ParDo.of(new TestUtils.ToTableRow())).
                apply(BigQueryIO.Write.
                        to(testTable).
                        withSchema(TestUtils.getTestTableSchema()));
        PipelineResult result = p.run();

        while (result.getState() != PipelineResult.State.RUNNING) {
            System.out.println(result.getState());
            Thread.sleep(1000);
        }
        Thread.sleep(1000 * 60 * 3);
        TestUtils.putRecords(testData);

        Thread.sleep(1000 * 60 * 1);

        assertThat(newHashSet(testData)).isEqualTo(newHashSet(BQ.get().readAllFrom(testTable)));
    }
}
