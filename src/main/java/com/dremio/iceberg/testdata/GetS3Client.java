package com.dremio.iceberg.testdata;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dremio.iceberg.s3.ApacheHttpConnectionUtil;
import com.dremio.iceberg.s3.NamedThreadFactory;
import com.dremio.iceberg.s3.S3Configs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;

public class GetS3Client {

    private static final ExecutorService threadPool = Executors.newCachedThreadPool(new NamedThreadFactory("s3-async-read-"));

    public static S3Client getSyncClient(S3Configs s3Configs) {
        return syncConfigClientBuilder(S3Client.builder(), s3Configs).build();
    }

    public static S3AsyncClient getAsyncClient(S3Configs s3Configs) {
        return asyncConfigClientBuilder(S3AsyncClient.builder(), s3Configs).build();
    }

    private static <T extends SdkSyncClientBuilder<T,?> & AwsClientBuilder<T,?>> T syncConfigClientBuilder(T builder, S3Configs s3Configs) {

        builder.credentialsProvider(getAsync2Provider(s3Configs))
                .httpClientBuilder(ApacheHttpConnectionUtil.initConnectionSettings());

        builder.region(Region.of(s3Configs.getRegion()));
        return builder;
    }

    private static <T extends AwsAsyncClientBuilder<T,?> & S3BaseClientBuilder<T,?>> T asyncConfigClientBuilder(T builder, S3Configs s3Configs) {

        builder.asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, threadPool))
                .credentialsProvider(getAsync2Provider(s3Configs));
        builder.region(Region.of(s3Configs.getRegion()));
        return builder;
    }

    private static AwsCredentialsProvider getAsync2Provider(S3Configs config) {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                config.getAccessKey(), config.getSecretKey()));
    }
}
