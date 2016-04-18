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

import org.apache.beam.sdk.io.kinesis.client.KinesisClientProvider;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;


/***
 *
 */
public class TestKinesisClientProvider implements KinesisClientProvider {
    private static final long serialVersionUID = 0L;

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String roleToAssume;

    public TestKinesisClientProvider() {
        accessKey = TestConfiguration.get().getClusterAwsAccessKey();
        secretKey = TestConfiguration.get().getClusterAwsSecretKey();
        region = TestConfiguration.get().getTestRegion();
        roleToAssume = TestConfiguration.get().getClusterAwsRoleToAssume();
    }

    private AWSCredentialsProvider getCredentialsProvider() {
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );

        if (roleToAssume != null) {
            return new
                    STSAssumeRoleSessionCredentialsProvider(
                    credentials, roleToAssume, "session"
            );
        }
        return new StaticCredentialsProvider(credentials);
    }

    @Override
    public AmazonKinesis get() {
        return new AmazonKinesisClient(getCredentialsProvider())
                .withRegion(Regions.fromName(region));
    }
}
