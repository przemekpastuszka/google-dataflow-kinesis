import com.amazonaws.auth.AWSCredentials;
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
    private static final long serialVersionUID = 0L;

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private transient SimplifiedKinesisClient client;

    public TestKinesisClientProvider() {
        accessKey = TestConfiguration.get().getAwsAccessKey();
        secretKey = TestConfiguration.get().getAwsSecretKey();
        region = TestConfiguration.get().getTestRegion();
    }

    @Override
    public synchronized SimplifiedKinesisClient get() {
        if (client == null) {
            client = createClient();
        }
        return client;
    }

    private SimplifiedKinesisClient createClient() {
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );
        AmazonKinesis kinesis = new AmazonKinesisClient(credentials)
                .withRegion(Regions.fromName(region));
        return new SimplifiedKinesisClient(kinesis);
    }
}
