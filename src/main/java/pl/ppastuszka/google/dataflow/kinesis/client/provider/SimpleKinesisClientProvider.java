package pl.ppastuszka.google.dataflow.kinesis.client.provider;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;


public class SimpleKinesisClientProvider implements KinesisClientProvider {
    @Override
    public AmazonKinesis getKinesisClient() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final AmazonKinesis INSTANCE = new AmazonKinesisClient(new EnvironmentVariableCredentialsProvider())
                .withRegion(Regions.EU_WEST_1);
    }
}
