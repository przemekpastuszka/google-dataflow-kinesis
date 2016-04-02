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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class TestConfiguration {
    private final Map<String, String> configuration;

    private TestConfiguration() {
        InputStream rawConfig = getClass().getResourceAsStream("/testconfig.json");
        try {
            configuration = new ObjectMapper().readValue(rawConfig, HashMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                rawConfig.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    public static TestConfiguration get() {
        return Holder.INSTANCE;
    }

    public String getTestStagingLocation() {
        return constructTestBucketPath("staging");
    }

    public String getTestTempLocation() {
        return constructTestBucketPath("tmp");
    }

    private String constructTestBucketPath(String directory) {
        return "gs://" + Paths.get(getTestBucket(), "dataflow", directory).toString();
    }

    public String getTestBucket() {
        return configuration.get("DATAFLOW_TEST_BUCKET");
    }

    public String getTestProject() {
        return configuration.get("DATAFLOW_TEST_PROJECT");
    }

    public String getTestDataset() {
        return configuration.get("DATAFLOW_TEST_DATASET");
    }

    public String getTestKinesisStream() {
        return configuration.get("TEST_KINESIS_STREAM");
    }

    public String getAwsSecretKey() {
        return configuration.get("AWS_SECRET_KEY");
    }

    public String getAwsAccessKey() {
        return configuration.get("AWS_ACCESS_KEY");
    }

    public String getTestPubSubTopic() {
        return configuration.get("PUB_SUB_TEST_TOPIC");
    }

    public String getClusterAwsAccessKey() {
        return getOrDefault("REMOTE_AWS_ACCESS_KEY", getAwsAccessKey());
    }

    public String getClusterAwsSecretKey() {
        return getOrDefault("REMOTE_AWS_SECRET_KEY", getAwsSecretKey());
    }

    private String getOrDefault(String key, String defaultValue) {
        if (configuration.containsKey(key)) {
            return configuration.get(key);
        }
        return defaultValue;
    }

    public String getClusterAwsRoleToAssume() {
        return configuration.get("REMOTE_AWS_ROLE_TO_ASSUME");
    }

    public String getTestRegion() {
        return configuration.get("TEST_KINESIS_STREAM_REGION");
    }

    private static class Holder {
        private static final TestConfiguration INSTANCE = new TestConfiguration();
    }
}
