import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.api.client.util.Lists.newArrayList;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import static java.util.Arrays.asList;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.List;
import pl.ppastuszka.google.dataflow.kinesis.client.provider.SimpleKinesisClientProvider;
import pl.ppastuszka.google.dataflow.kinesis.source.KinesisDataflowSource;

/***
 *
 */
public class TestUtils {

    private static final SecureRandom random = new SecureRandom();
    private static Bigquery bigquery;

    private static Bigquery getBigquery() {
        if (bigquery == null) {
            bigquery = buildBigQuery();
        }
        return bigquery;
    }

    private static Bigquery buildBigQuery() {
        try {
            JacksonFactory jaksonFactory = JacksonFactory.getDefaultInstance();
            NetHttpTransport httpTransport = newTrustedTransport();
            return new Bigquery.Builder(httpTransport, jaksonFactory,
                    GoogleCredential.getApplicationDefault(httpTransport, jaksonFactory)).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


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
                setProjectId(getTestProject()).
                setDatasetId(getTestDataset()).
                setTableId(getTestTableId());
    }

    public static void deleteTableIfExists(TableReference reference) throws IOException {
        getBigquery().tables().delete(
                reference.getProjectId(),
                reference.getDatasetId(),
                reference.getTableId());
    }

    public static String getTestStagingLocation() {
        return constructTestBucketPath("staging");
    }

    public static String getTestTempLocation() {
        return constructTestBucketPath("tmp");
    }

    private static String constructTestBucketPath(String directory) {
        return "gs://" + Paths.get(getTestBucket(), "dataflow", directory).toString();
    }

    public static String getTestBucket() {
        return System.getenv("DATAFLOW_TEST_BUCKET");
    }

    public static String getTestProject() {
        return System.getenv("DATAFLOW_TEST_PROJECT");
    }

    public static String getTestDataset() {
        return System.getenv("DATAFLOW_TEST_DATASET");
    }

    public static String getTestTableId() {
        return randomString();
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

    static class ToTableRow extends DoFn<String, TableRow> {

        @Override
        public void processElement(ProcessContext c) throws Exception {
            c.output(new TableRow().setF(asList(new TableCell().setV(c.element()))));
        }
    }

    static class ByteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            c.output(new String(c.element(), Charsets.UTF_8));
        }
    }
}
