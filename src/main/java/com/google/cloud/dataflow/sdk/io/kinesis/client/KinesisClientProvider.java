package com.google.cloud.dataflow.sdk.io.kinesis.client;

import com.amazonaws.services.kinesis.AmazonKinesis;
import java.io.Serializable;

/**
 * Created by ppastuszka on 10.03.16.
 */
public interface KinesisClientProvider extends Serializable {
    AmazonKinesis get();
}
