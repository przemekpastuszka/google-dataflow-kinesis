package utils;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.api.client.util.Lists.newArrayList;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import static java.util.Arrays.asList;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
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

    public static TableSchema getTestTableSchema() {
        return new TableSchema().
                setFields(asList(
                        new TableFieldSchema()
                                .setName("a")
                                .setType("STRING")));
    }

    public static TableReference getTestTableReference() {
        return new TableReference().
                setProjectId(TestConfiguration.get().getTestProject()).
                setDatasetId(TestConfiguration.get().getTestDataset()).
                setTableId(getTestTableId());
    }

    public static String getTestTableId() {
        return randomString();
    }

    public static KinesisDataflowSource getTestKinesisSource() {
        return new KinesisDataflowSource(
                getTestKinesisClientProvider(),
                TestConfiguration.get().getTestKinesisStream(),
                ShardIteratorType.LATEST);
    }

    public static AWSCredentialsProvider getTestAwsCredentialsProvider() {
        return new StaticCredentialsProvider(new BasicAWSCredentials(
                TestConfiguration.get().getAwsAccessKey(),
                TestConfiguration.get().getAwsSecretKey()
        ));
    }

    public static void putRecordsWithKinesisProducer(List<String> data) {


        KinesisProducer producer = new KinesisProducer(
                new KinesisProducerConfiguration().
                        setCredentialsProvider(getTestAwsCredentialsProvider()).
                        setRegion(TestConfiguration.get().getTestRegion())
        );
        List<ListenableFuture<UserRecordResult>> futures = newArrayList();
        for (String s : data) {
            ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                    TestConfiguration.get().getTestKinesisStream(),
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

    public static void putRecordsOldStyle(List<String> data) {
                List<List<String>> partitions = Lists.partition(data, 499);

        AmazonKinesisClient client = new AmazonKinesisClient
                (getTestAwsCredentialsProvider())
                .withRegion(
                        Regions.fromName(TestConfiguration.get().getTestRegion()));
        for (List<String> partition : partitions) {
            List<PutRecordsRequestEntry> putRecords = newArrayList();
            for (String row : partition) {
                putRecords.add(new PutRecordsRequestEntry().
                        withData(ByteBuffer.wrap(row.getBytes(Charsets.UTF_8))).
                        withPartitionKey(Integer.toString(row.hashCode()))

                );
            }

            PutRecordsResult result = client.putRecords(
                    new PutRecordsRequest().
                            withStreamName(TestConfiguration.get().getTestKinesisStream()).
                            withRecords(putRecords)
            );
            if (result.getFailedRecordCount() > 0) {
                throw new RuntimeException("Failed to upload rows");
            }
        }
    }

    private static TestKinesisClientProvider getTestKinesisClientProvider() {
        return new TestKinesisClientProvider();
    }

    /***
     *
     */
    public static class ToTableRow extends DoFn<String, TableRow> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new TableRow().set("a", c.element()));
        }
    }

    /***
     *
     */
    public static class ByteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new String(c.element(), Charsets.UTF_8));
        }
    }
}
