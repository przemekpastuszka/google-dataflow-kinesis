package utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
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
    private final String roleToAssume;
    private transient SimplifiedKinesisClient client;

    public TestKinesisClientProvider() {
        accessKey = TestConfiguration.get().getClusterAwsAccessKey();
        secretKey = TestConfiguration.get().getClusterAwsSecretKey();
        region = TestConfiguration.get().getTestRegion();
        roleToAssume = TestConfiguration.get().getClusterAwsRoleToAssume();
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

        STSAssumeRoleSessionCredentialsProvider provider = new
                STSAssumeRoleSessionCredentialsProvider(
                credentials, roleToAssume, "session"
        );

        AmazonKinesis kinesis = new AmazonKinesisClient(provider)
                .withRegion(Regions.fromName(region));
        return new SimplifiedKinesisClient(kinesis);
    }
}
