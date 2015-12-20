package pl.ppastuszka.google.dataflow.kinesis.client;

import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxyFactory;
import java.io.Serializable;

/**
 * Created by ppastuszka on 05.12.15.
 */
public interface SerializableKinesisProxyFactory extends IKinesisProxyFactory, Serializable {
}
