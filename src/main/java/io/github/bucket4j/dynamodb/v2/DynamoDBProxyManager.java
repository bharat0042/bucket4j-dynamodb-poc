/*-
 * ========================LICENSE_START=================================
 * Bucket4j
 * %%
 * Copyright (C) 2015 - 2021 Vladimir Bukhtoyarov
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package io.github.bucket4j.dynamodb.v2;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.Refill;
import io.github.bucket4j.distributed.BucketProxy;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.Bandwidth;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public final class DynamoDBProxyManager {

    public static BaseDynamoDBProxyManager<String> stringKey(AmazonDynamoDB db, String table, ClientSideConfig config) {
        return new StringDynamoDBProxyManager(db, table, config);
    }


    public static BaseDynamoDBProxyManager<Long> longKey(AmazonDynamoDB db, String table, ClientSideConfig config) {
        return new LongDynamoDBProxyManager(db, table, config);
    }

    private DynamoDBProxyManager() {}

    public static BucketProxy getBucketForString(String key) {
        BasicAWSCredentials credentials = new BasicAWSCredentials("accessKey", "secretKey");
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
                .build();
//  only for testing locally with dynamodb, can be remove when not needed
//        CreateTableRequest request = new CreateTableRequest()
//                .withTableName("my-table")
//                .withKeySchema(new KeySchemaElement("key", KeyType.HASH))
//                .withAttributeDefinitions(new AttributeDefinition("key", ScalarAttributeType.S))
//                .withProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
//
//        CreateTableResult table = client.createTable(request);
//        System.out.println(table.getTableDescription().getTableName());
//        ListTablesResult result = client.listTables();
//        List<String> tableNames = result.getTableNames();
//        for (String tableName : tableNames) {
//            System.out.println(tableName);
//        }

        BaseDynamoDBProxyManager<String> proxyManager = stringKey(client, "my-table", ClientSideConfig.getDefault());

        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.classic(4, Refill.intervally(10, Duration.ofHours(1))))
                .build();

        BucketProxy bucket = proxyManager.builder().build(key, configuration);
        return bucket;
    }

    public static void main(String[] args) {
        BucketProxy bucketProxy = getBucketForString("thisisakey3");
        System.out.println(bucketProxy.getAvailableTokens());
        for(int i = 0; i < 10; i++) {
            ConsumptionProbe probe = bucketProxy.tryConsumeAndReturnRemaining(1);
            if (probe.isConsumed()) {
                System.out.println("All good");
                continue;
            }
            long waitForRefill = probe.getNanosToWaitForRefill() / 1_000_000_000;
            System.out.println("Not all good. Wait for : " + String.valueOf(waitForRefill));
        }
    }
}
