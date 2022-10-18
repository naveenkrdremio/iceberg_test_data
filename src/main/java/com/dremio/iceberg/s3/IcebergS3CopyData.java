/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.iceberg.s3;

import static com.dremio.iceberg.utils.Constants.S3_ACCESS_KEY;
import static com.dremio.iceberg.utils.Constants.FILE_COUNT_END;
import static com.dremio.iceberg.utils.Constants.FILE_COUNT_START;
import static com.dremio.iceberg.utils.Constants.KEY;
import static com.dremio.iceberg.utils.Constants.PARTITION_END;
import static com.dremio.iceberg.utils.Constants.PARTITION_KEY;
import static com.dremio.iceberg.utils.Constants.PARTITION_START;
import static com.dremio.iceberg.utils.Constants.S3_SECRET_KEY;
import static com.dremio.iceberg.utils.Constants.SOURCE_BUCKET;
import static com.dremio.iceberg.utils.Constants.SOURCE_REGION;
import static com.dremio.iceberg.utils.Constants.TABLE_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dremio.iceberg.testdata.GetS3Client;

import software.amazon.awssdk.services.s3.S3Client;

public class IcebergS3CopyData {

    private static final ExecutorService threadPool = Executors.newFixedThreadPool(100,new NamedThreadFactory("s3-copy-"));

    public static void main(String[] args) {
        S3Configs s3Configs = new S3Configs(S3_ACCESS_KEY, S3_SECRET_KEY, SOURCE_BUCKET, SOURCE_REGION);
        //
        S3Client s3Client = GetS3Client.getSyncClient(s3Configs);


        Map<String, String> map = sizeLocations();
        map.forEach((key, value) -> {
            String sourceFile = SOURCE_BUCKET+"/"+value;
            List<CompletableFuture> completableFutures = new ArrayList<>();
            for (int i = PARTITION_START; i < PARTITION_END; i++) {
                for (int j = FILE_COUNT_START; j < FILE_COUNT_END; j++) {
                    S3SyncCopy s3SyncCopy = new S3SyncCopy(s3Client, sourceFile,KEY+"/"+PARTITION_KEY+"="+ i +"/"+key+"_"+ j +".parquet" );
                    CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(s3SyncCopy, threadPool);
                    completableFutures.add(voidCompletableFuture);
                }
            }
            int count=0;
            for (CompletableFuture future : completableFutures) {
                while (!future.isDone()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                count++;
            }
            System.out.println("Key -> " + key + " , value -> " + value + " , count -> "+ count);
            try {
                Thread.sleep(100000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("Created Table "+ TABLE_NAME + "with " + "Partitions: "+ (PARTITION_END-PARTITION_START) );

    }

    private static Map<String, String > sizeLocations() {
        //1MB - s3://dataplane/testdata/optimize/one_mb_np/1cb660b8-f4ae-199d-fb4f-6e46407d7900/0_0_0.parquet
        //10mb - s3://dataplane/testdata/optimize/ten_mb_np/1cb59c26-9fcc-4b0b-3234-054e52edde00/0_0_0.parquet
        //20mb - s3://dataplane/testdata/optimize/twenty_mb_np/1cb58b89-feda-230a-1aeb-97a72ec53900/0_0_0.parquet
        //30mb - s3://dataplane/testdata/optimize/partitioned/store_sales_partitioned/data/ss_sold_date_sk=2450828/00653-653-ab014aa5-92bb-476b-a344-5d9a780dd043-00001.parquet
        //50MB - s3://dataplane/testdata/optimize/fifty_mb_np/1cb589ab-1a2e-aa26-dea7-76dd78642b00/2_0_0.parquet
        //80MB - s3://dataplane/testdata/optimize/t2/1cb57518-67b8-a35c-9989-c8d3c8d19100/0_0_0.parquet
        //160MB - s3://dataplane/testdata/optimize/t2/1cb51008-19f9-e8e3-79ca-bf5d8a072c00/0_0_0.parquet
        //256MB - s3://dataplane/testdata/optimize/five12/1cb50ef3-64ac-f550-af6a-982e74e30700/2_0_0.parquet
        //512MB - s3://dataplane/testdata/optimize/five12/1cb50e35-7930-b3cb-1df9-d7f4ec0cb500/0_0_0.parquet
        //1024 - s3://dataplane/testdata/optimize/t2/1cb69530-87c6-396f-5ac9-83287b057100/0_0_0.parquet
        Map<String, String> map = new HashMap<>();
        map.put("1mb","testdata/optimize/one_mb_np/1cb660b8-f4ae-199d-fb4f-6e46407d7900/0_0_0.parquet");
        map.put("10mb","testdata/optimize/ten_mb_np/1cb59c26-9fcc-4b0b-3234-054e52edde00/0_0_0.parquet");
        map.put("20mb","testdata/optimize/twenty_mb_np/1cb58b89-feda-230a-1aeb-97a72ec53900/0_0_0.parquet");
        map.put("30mb","testdata/optimize/partitioned/store_sales_partitioned/data/ss_sold_date_sk=2450828/00653-653-ab014aa5-92bb-476b-a344-5d9a780dd043-00001.parquet");
        map.put("50mb","testdata/optimize/fifty_mb_np/1cb589ab-1a2e-aa26-dea7-76dd78642b00/2_0_0.parquet");
        map.put("80mb","testdata/optimize/t2/1cb57518-67b8-a35c-9989-c8d3c8d19100/0_0_0.parquet");
        map.put("160mb","testdata/optimize/t2/1cb51008-19f9-e8e3-79ca-bf5d8a072c00/0_0_0.parquet");
        map.put("256mb","testdata/optimize/five12/1cb50ef3-64ac-f550-af6a-982e74e30700/2_0_0.parquet");
        map.put("512mb","testdata/optimize/five12/1cb50e35-7930-b3cb-1df9-d7f4ec0cb500/0_0_0.parquet");
        map.put("1024mb","testdata/optimize/t2/1cb69530-87c6-396f-5ac9-83287b057100/0_0_0.parquet");
        return map;
    }

}
