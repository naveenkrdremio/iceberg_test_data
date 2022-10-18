package com.dremio.iceberg.s3;


import static com.dremio.iceberg.utils.Constants.DEST_BUCKET;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3SyncCopy implements Runnable {

    private S3Client s3Client;
    private String sourceKey;
    private String destKey;

    public S3SyncCopy(S3Client s3Client, String sourceKey, String destKey) {
        this.s3Client = s3Client;
        this.sourceKey = sourceKey;
        this.destKey = destKey;
    }

    @Override
    public void run() {
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .copySource(sourceKey)
                .destinationBucket(DEST_BUCKET)
                .destinationKey(destKey)
                .build();

        try {
            CopyObjectResponse copyRes = s3Client.copyObject(copyReq);
        } catch (S3Exception e) {
            System.out.println("Error -> " + e.awsErrorDetails().errorMessage());
            //System.exit(1);
        }
    }
}
