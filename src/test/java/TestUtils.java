import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Duration;
import pl.ppastuszka.google.dataflow.kinesis.client.SimplifiedKinesisClient;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Collections2.transform;

/**
 * Created by ppastuszka on 05.12.15.
 */
public class TestUtils {

    private static final SecureRandom random = new SecureRandom();

    public static String randomString() {
        return new BigInteger(130, random).toString(32);
    }

    public static List<String> randomStrings(int howMany) {
        List<String> data = newArrayList();
        for (int i = 0; i < howMany; ++i) {
            data.add(TestUtils.randomString());
        }
        return data;
    }

    public static KinesisDataflowSource getTestKinesisSource() {
        return new KinesisDataflowSource(
                getTestKinesisClientProvider(),
                getTestKinesisStream(),
                ShardIteratorType.LATEST);
    }

    public static AWSCredentialsProvider getTestAwsCredentialsProvider() {
        return new EnvironmentVariableCredentialsProvider();
    }

    private static String getTestKinesisStream() {
        return System.getenv("TEST_KINESIS_STREAM");
    }

    private static String getTestRegion() {return System.getenv("TEST_KINESIS_STREAM_REGION");}

    public static void putRecords(List<String> data) {
        KinesisProducer producer = new KinesisProducer(
                new KinesisProducerConfiguration().
                        setCredentialsProvider(getTestAwsCredentialsProvider()).
                        setRegion(getTestRegion())
        );
        List<ListenableFuture<UserRecordResult>> futures = newArrayList();
        for (String s : data) {
            try {
                ListenableFuture<UserRecordResult> future = producer.addUserRecord(getTestKinesisStream(), Integer.toString(s.hashCode()), ByteBuffer.wrap(s.getBytes("UTF-8")));
                futures.add(future);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        for (ListenableFuture<UserRecordResult> future : futures) {
            try {
                UserRecordResult result = future.get();
                if (!result.isSuccessful()) {
                    throw new RuntimeException("Failed to send record");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static SimpleKinesisClientProvider getTestKinesisClientProvider() {
        return new SimpleKinesisClientProvider();
    }

//    public static <T, M extends UnboundedSource.CheckpointMark> BoundedSource<T> toBounded(UnboundedSource<T, M> s, long
//            maxRecords) throws Exception {
//        Class<? extends BoundedSource<T>> clazz = (Class<? extends BoundedSource<T>>) Class.forName("com.google.cloud.dataflow" +
//                ".sdk.io" +
//                ".BoundedReadFromUnboundedSource$UnboundedToBoundedSourceAdapter");
//
//        Constructor<? extends BoundedSource<T>> constructor = clazz.getDeclaredConstructor(UnboundedSource.class,
//                Long.TYPE, Duration.class);
//        constructor.setAccessible(true);
//        return constructor.newInstance(s, maxRecords, null);
//    }

}
