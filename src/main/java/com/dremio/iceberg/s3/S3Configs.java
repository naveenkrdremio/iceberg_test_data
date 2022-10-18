package com.dremio.iceberg.s3;

public class S3Configs {

    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    private final String region;

    public S3Configs(String accessKey, String secretKey, String bucket, String region) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.region = region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucket() {
        return bucket;
    }

    public String getRegion() {
        return region;
    }
}
