package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator
        .registerTransformTranslator;
import static com.google.cloud.dataflow.sdk.util.StringUtils.approximateSimpleName;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.google.cloud.dataflow.sdk.io.kinesis.source.KinesisDataflowSource;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator;
import com.google.cloud.dataflow.sdk.runners.dataflow.CustomSources;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.joda.time.Duration;

/**
 * Created by ppastuszka on 10.03.16.
 */
public class KinesisIO {
    static {
        registerTransformTranslator(Read.Unbound.class, new DataflowPipelineTranslator
                .TransformTranslator<Read.Unbound>() {
            @Override
            public void translate(Read.Unbound transform, DataflowPipelineTranslator
                    .TranslationContext context) {
                CustomSources.translateReadHelper(transform.getSource(), transform, context);
            }
        });
    }

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

        public Unbound using(KinesisClientProvider kinesisClientProvider) {
            return new Unbound(null, new KinesisDataflowSource(kinesisClientProvider, streamName,
                    initialPosition));
        }

        public Unbound using(String awsAccessKey, String awsSecretKey, Regions region) {
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

        public static class Unbound extends PTransform<PInput, PCollection<byte[]>> {
            private final KinesisDataflowSource source;

            private Unbound(String name, KinesisDataflowSource source) {
                super(name);
                this.source = source;
            }

            public Unbound named(String name) {
                return new Unbound(name, source);
            }


            public BoundedReadFromUnboundedSource<byte[]> withMaxNumRecords(long maxNumRecords) {
                return new BoundedReadFromUnboundedSource<>(source, maxNumRecords, null);
            }


            public BoundedReadFromUnboundedSource<byte[]> withMaxReadTime(Duration maxReadTime) {
                return new BoundedReadFromUnboundedSource<>(source, Long.MAX_VALUE, maxReadTime);
            }

            @Override
            protected Coder<byte[]> getDefaultOutputCoder() {
                return source.getDefaultOutputCoder();
            }

            @Override
            public final PCollection<byte[]> apply(PInput input) {
                return PCollection.createPrimitiveOutputInternal(
                        input.getPipeline(), WindowingStrategy.globalDefault(), PCollection
                                .IsBounded
                                .UNBOUNDED);
            }


            public UnboundedSource<byte[], ?> getSource() {
                return source;
            }

            @Override
            public String getKindString() {
                return "Unbound(" + approximateSimpleName(source.getClass()) + ")";
            }
        }
    }
}
