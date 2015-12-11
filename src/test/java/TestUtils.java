import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import org.joda.time.Duration;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

import java.lang.reflect.Constructor;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class TestUtils {

    public static KinesisDataflowSource getTestKinesisSource() {
        return new KinesisDataflowSource(new SimpleKinesisClientProvider(), System.getenv("TEST_KINESIS_STREAM"),
                ShardIteratorType.LATEST);
    }

    public static <T, M extends UnboundedSource.CheckpointMark> BoundedSource<T> toBounded(UnboundedSource<T, M> s, long
            maxRecords) throws Exception {
        Class<? extends BoundedSource<T>> clazz = (Class<? extends BoundedSource<T>>) Class.forName("com.google.cloud.dataflow" +
                ".sdk.io" +
                ".BoundedReadFromUnboundedSource$UnboundedToBoundedSourceAdapter");

        Constructor<? extends BoundedSource<T>> constructor = clazz.getDeclaredConstructor(UnboundedSource.class,
                Long.TYPE, Duration.class);
        constructor.setAccessible(true);
        return constructor.newInstance(s, maxRecords, null);
    }

}
