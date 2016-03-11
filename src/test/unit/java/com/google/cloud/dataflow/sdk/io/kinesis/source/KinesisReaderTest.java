package com.google.cloud.dataflow.sdk.io.kinesis.source;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.MultiShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.SingleShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .MultiShardCheckpointGenerator;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.MyOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.Record;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static java.util.Arrays.asList;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Created by ppastuszka on 12.12.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderTest {
    @Mock
    private SimplifiedKinesisClient kinesis;
    @Mock
    private MultiShardCheckpointGenerator generator;
    @Mock
    private SingleShardCheckpoint firstCheckpoint, secondCheckpoint;
    @Mock
    private ShardRecordsIterator firstIterator, secondIterator;
    @Mock
    private Record a, b, c, d;

    private KinesisReader reader;

    @Before
    public void setUp() throws IOException {
        when(generator.generate(kinesis)).thenReturn(new MultiShardCheckpoint(
                asList(firstCheckpoint, secondCheckpoint)
        ));
        when(firstCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(firstIterator);
        when(secondCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(secondIterator);
        when(firstIterator.next()).thenReturn(MyOptional.<Record>absent());
        when(secondIterator.next()).thenReturn(MyOptional.<Record>absent());

        when(a.getSequenceNumber()).thenReturn("a");
        when(b.getSequenceNumber()).thenReturn("b");
        when(c.getSequenceNumber()).thenReturn("c");
        when(d.getSequenceNumber()).thenReturn("d");

        reader = new KinesisReader(kinesis, generator, null, null);
    }

    @Test
    public void startReturnsFalseIfNoDataAtTheBeginning() throws IOException {
        assertThat(reader.start()).isFalse();
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsNoSuchElementExceptionIfNoData() throws IOException {
        reader.start();
        reader.getCurrent();
    }

    @Test
    public void startReturnsTrueIfSomeDataAvailable() throws IOException {
        when(firstIterator.next()).
                thenReturn(Optional.of(a)).
                thenReturn(MyOptional.<Record>absent());

        assertThat(reader.start()).isTrue();
    }

    @Test
    public void readsThroughAllDataAvailable() throws IOException {
        when(firstIterator.next()).
                thenReturn(MyOptional.<Record>absent()).
                thenReturn(Optional.of(a)).
                thenReturn(MyOptional.<Record>absent()).
                thenReturn(Optional.of(b)).
                thenReturn(MyOptional.<Record>absent());

        when(secondIterator.next()).
                thenReturn(Optional.of(c)).
                thenReturn(MyOptional.<Record>absent()).
                thenReturn(Optional.of(d)).
                thenReturn(MyOptional.<Record>absent());

        assertThat(reader.start()).isTrue();
        assertThat(fromBytes(reader.getCurrentRecordId())).isEqualTo("c");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrentRecordId())).isEqualTo("a");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrentRecordId())).isEqualTo("d");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrentRecordId())).isEqualTo("b");
        assertThat(reader.advance()).isFalse();
    }

    private String fromBytes(byte[] bytes) {
        return new String(bytes, Charsets.UTF_8);
    }


}
