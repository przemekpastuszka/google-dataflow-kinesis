/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utils;

import static com.google.api.client.googleapis.javanet.GoogleNetHttpTransport.newTrustedTransport;
import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkArgument;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists.partition;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;

import static java.util.Arrays.asList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by ppastuszka on 14.12.15.
 */
public class PubSubUtil {
    public static final int MAX_NUM_OF_RECRODS = 1000;
    private final Pubsub pubsub;

    PubSubUtil() {
        try {
            JacksonFactory jaksonFactory = JacksonFactory.getDefaultInstance();
            NetHttpTransport httpTransport = newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault
                    (httpTransport, jaksonFactory).createScoped(PubsubScopes.all());
            this.pubsub = new Pubsub.Builder(httpTransport, jaksonFactory, credential).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static PubSubUtil get() {
        return Holder.INSTANCE;
    }

    public static void main(String[] args) {
        get().startSendingRecordsToPubSub(asList("test"));
    }

    public List<Future<?>> startSendingRecordsToPubSub(List<String> data) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<Future<?>> futures = com.google.cloud.dataflow.sdk.repackaged.com.google.common
                .collect.Lists.newArrayList();

        for (final List<String> partition : partition(data, PubSubUtil.MAX_NUM_OF_RECRODS)) {
            Future<?> future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        sendDataTo(TestConfiguration.get().getTestPubSubTopic(), partition);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }
        return futures;
    }

    public void sendDataTo(String topic, List<String> data) throws IOException {
        checkArgument(data.size() <= MAX_NUM_OF_RECRODS);
        List<PubsubMessage> messages = Lists.newArrayList();
        for (String dataPart : data) {
            messages.add(new PubsubMessage().encodeData(dataPart.getBytes("UTF-8")));
        }
        pubsub.projects().topics
                ().publish(topic, new PublishRequest().setMessages(messages)).execute();
    }

    private static class Holder {
        private static final PubSubUtil INSTANCE = new PubSubUtil();
    }
}
