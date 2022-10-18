package com.dremio.iceberg.utils;

public final class Constants {

    public static final String S3_ACCESS_KEY = "";
    public static final String S3_SECRET_KEY = "";
    public static final String SOURCE_BUCKET = "dataplane";
    public static final String DEST_BUCKET = "dataplane";
    public static final String SOURCE_REGION = "us-west-2";
    public static final String KEY = "testdata/optimize/mixed_size_dataset";
    public static final String PARTITION_KEY ="ss_sold_date_sk";
    public static final String TABLE_NAME = "small_un_partitioned_table_1";
    public static final int PARTITION_START = 0;
    public static final int PARTITION_END = 1;
    public static final int FILE_COUNT_START = 0;
    public static final int FILE_COUNT_END = 100;
    public static final String[] FILE_SIZES = {"1mb","10mb","20mb","30mb","50mb","80mb","160mb","256mb","512mb","1024mb"};

    public static final boolean IS_PARTITIONED = false;

}
