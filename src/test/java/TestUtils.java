import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

/***
 *
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

    private static String getTestRegion() {
        return System.getenv("TEST_KINESIS_STREAM_REGION");
    }

    public static void putRecords(List<String> data) {
        KinesisProducer producer = new KinesisProducer(
                new KinesisProducerConfiguration().
                        setCredentialsProvider(getTestAwsCredentialsProvider()).
                        setRegion(getTestRegion())
        );
        List<ListenableFuture<UserRecordResult>> futures = newArrayList();
        for (String s : data) {
            ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                    getTestKinesisStream(),
                    Integer.toString(s.hashCode()),
                    ByteBuffer.wrap(s.getBytes(Charsets.UTF_8)));
            futures.add(future);
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


}
