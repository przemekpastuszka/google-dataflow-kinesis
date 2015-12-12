package pl.ppastuszka.google.dataflow.kinesis.client.provider;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import pl.ppastuszka.google.dataflow.kinesis.client.SimplifiedKinesisClient;


public class SimpleKinesisClientProvider implements KinesisClientProvider {
    @Override
    public SimplifiedKinesisClient get() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final AmazonKinesis KINESIS = new AmazonKinesisClient(new EnvironmentVariableCredentialsProvider())
                .withRegion(Regions.EU_WEST_1);
        private static final SimplifiedKinesisClient INSTANCE = new SimplifiedKinesisClient(KINESIS);
    }
}
