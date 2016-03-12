package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.google.cloud.dataflow.sdk.io.kinesis.source.KinesisDataflowSource;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * Created by ppastuszka on 10.03.16.
 */
public class KinesisIO {
    public static class Read {

        private final String streamName;
        private final InitialPositionInStream initialPosition;

        public Read(String streamName, InitialPositionInStream initialPosition) {
            this.streamName = streamName;
            this.initialPosition = initialPosition;
        }

        public static Read from(String streamName, InitialPositionInStream initialPosition) {
            return new Read(streamName, initialPosition);
        }

        public com.google.cloud.dataflow.sdk.io.Read.Unbounded<byte[]> using
                (KinesisClientProvider kinesisClientProvider) {
            return com.google.cloud.dataflow.sdk.io.Read.from(
                    new KinesisDataflowSource(kinesisClientProvider, streamName,
                            initialPosition));
        }

        public com.google.cloud.dataflow.sdk.io.Read.Unbounded<byte[]> using(String awsAccessKey,
                                                                             String awsSecretKey,
                                                                             Regions region) {
            return using(new BasicKinesisProvider(awsAccessKey, awsSecretKey, region));
        }

        private static class BasicKinesisProvider implements KinesisClientProvider {

            private final String accessKey;
            private final String secretKey;
            private final Regions region;

            private BasicKinesisProvider(String accessKey, String secretKey, Regions region) {
                this.accessKey = accessKey;
                this.secretKey = secretKey;
                this.region = region;
            }


            private AWSCredentialsProvider getCredentialsProvider() {
                return new StaticCredentialsProvider(new BasicAWSCredentials(
                        accessKey,
                        secretKey
                ));

            }

            @Override
            public AmazonKinesis get() {
                return new AmazonKinesisClient(getCredentialsProvider()).withRegion(region);
            }
        }
    }
}
