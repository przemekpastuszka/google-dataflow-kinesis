import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import pl.ppastuszka.google.dataflow.kinesis.client.SimplifiedKinesisClient;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.KinesisClientProvider;

/***
 *
 */
public class TestKinesisClientProvider implements KinesisClientProvider {
    @Override
    public SimplifiedKinesisClient get() {
        return Holder.INSTANCE;
    }

    private static class Holder {

        private static final AmazonKinesis KINESIS = new AmazonKinesisClient(
                new BasicAWSCredentials(
                        TestConfiguration.get().getAwsAccessKey(),
                        TestConfiguration.get().getAwsSecretKey()
                )).
                withRegion(Regions.fromName(TestConfiguration.get().getTestRegion()));

        private static final SimplifiedKinesisClient INSTANCE = new SimplifiedKinesisClient
                (KINESIS);
    }
}
