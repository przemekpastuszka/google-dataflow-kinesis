import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class BasicSourceTests {
    @Test
    public void test() throws Exception {
        BoundedSource<byte[]> bounded = TestUtils.toBounded(TestUtils.getTestKinesisSource(), 5);
//        SourceTestUtils.assertSplitAtFractionExhaustive(bounded, PipelineOptionsFactory.create());
        System.out.println(SourceTestUtils.readFromSource(bounded, PipelineOptionsFactory.create()));
    }
}
