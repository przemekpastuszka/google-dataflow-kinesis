package utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import pl.ppastuszka.google.dataflow.kinesis.client.SerializableKinesisProxyFactory;

/***
 *
 */
public class TestKinesisClientProvider implements SerializableKinesisProxyFactory {
    private static final long serialVersionUID = 0L;

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String roleToAssume;

    public TestKinesisClientProvider() {
        accessKey = TestConfiguration.get().getClusterAwsAccessKey();
        secretKey = TestConfiguration.get().getClusterAwsSecretKey();
        region = TestConfiguration.get().getTestRegion();
        roleToAssume = TestConfiguration.get().getClusterAwsRoleToAssume();
    }

    private AmazonKinesis getKinesisClient(AWSCredentialsProvider provider) {
        return new AmazonKinesisClient(provider)
                .withRegion(Regions.fromName(region));
    }

    private AWSCredentialsProvider getCredentialsProvider() {
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );

        return new
                STSAssumeRoleSessionCredentialsProvider(
                credentials, roleToAssume, "session"
        );
    }

    @Override
    public IKinesisProxy getProxy(String streamName) {
        AWSCredentialsProvider credentialsProvider = getCredentialsProvider();
        return new KinesisProxy(
                streamName,
                credentialsProvider,
                getKinesisClient(credentialsProvider),
                1000L,
                50
        );
    }
}
