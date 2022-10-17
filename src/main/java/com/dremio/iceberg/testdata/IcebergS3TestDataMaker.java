/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.iceberg.testdata;

import static com.dremio.iceberg.testdata.Constants.ACCESS_KEY;
import static com.dremio.iceberg.testdata.Constants.SECRET_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

public class IcebergS3TestDataMaker {

    public static void main(String[] args) {
        IcebergS3TestDataMaker testDataMaker = new IcebergS3TestDataMaker("s3a://dataplane/testdata/optimize/table_with_365_partitions");
        testDataMaker.createSnapshotWithCopies();
    }

    private static final Logger LOGGER = Logger.getLogger(IcebergTestDataMaker.class.getName());
    private final String tableLocation;

    private final PartitionSpec partitionSpec;
    private final Schema schema;

    private final Configuration conf = getConf();
    private final Table table;

    public IcebergS3TestDataMaker(String tableLocation) {
        this.tableLocation = tableLocation;
        //This schema is based on the files which got created inside the folders:
        this.schema = new Schema(
                Types.NestedField.optional(0, "ss_sold_date_sk", Types.IntegerType.get()),
                Types.NestedField.optional(1, "ss_sold_time_sk", Types.IntegerType.get()),
                Types.NestedField.optional(2, "ss_item_sk", Types.IntegerType.get()),
                Types.NestedField.optional(3, "ss_customer_sk", Types.IntegerType.get()),
                Types.NestedField.optional(4, "ss_cdemo_sk", Types.IntegerType.get()),
                Types.NestedField.optional(5, "ss_hdemo_sk", Types.IntegerType.get()),
                Types.NestedField.optional(6, "ss_addr_sk", Types.IntegerType.get()),
                Types.NestedField.optional(7, "ss_store_sk", Types.IntegerType.get()),
                Types.NestedField.optional(8, "ss_promo_sk", Types.IntegerType.get()),
                Types.NestedField.optional(9, "ss_ticket_number", Types.IntegerType.get()),
                Types.NestedField.optional(10, "ss_quantity", Types.IntegerType.get()),
                Types.NestedField.optional(11, "ss_wholesale_cost", Types.DoubleType.get()),
                Types.NestedField.optional(12, "ss_list_price", Types.DoubleType.get()),
                Types.NestedField.optional(13, "ss_sales_price", Types.DoubleType.get()),
                Types.NestedField.optional(14, "ss_ext_discount_amt", Types.DoubleType.get()),
                Types.NestedField.optional(15, "ss_ext_sales_price", Types.DoubleType.get()),
                Types.NestedField.optional(16, "ss_ext_wholesale_cost", Types.DoubleType.get()),
                Types.NestedField.optional(17, "ss_ext_list_price", Types.DoubleType.get()),
                Types.NestedField.optional(18, "ss_ext_tax", Types.DoubleType.get()),
                Types.NestedField.optional(19, "ss_coupon_amt", Types.DoubleType.get()),
                Types.NestedField.optional(20, "ss_net_paid", Types.DoubleType.get()),
                Types.NestedField.optional(21, "ss_net_paid_inc_tax", Types.DoubleType.get()),
                Types.NestedField.optional(22, "ss_net_profit", Types.DoubleType.get()));
        this.partitionSpec = PartitionSpec.builderFor(schema).withSpecId(0).identity("ss_sold_date_sk").build();

        this.table = new HadoopTables(conf).create(schema, partitionSpec, this.tableLocation);
        LOGGER.info("Created table " + tableLocation);
    }

    private Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", ACCESS_KEY);
        conf.set("fs.s3a.secret.key", SECRET_KEY);
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return conf;
    }

    private void createSnapshotWithCopies() {
        AppendFiles tableAppend = table.newAppend();
        for (FileMetadata file: getFiles()) {
            for (int partitionId = 0; partitionId < 365; partitionId++) {
                for (int fileId = 0; fileId < 100; fileId++) {
                    DataFile dataFile = DataFiles.builder(partitionSpec)
                            .withPath("s3://dataplane/testdata/optimize/mixed_size_dataset/ss_sold_date_sk="+partitionId+"/"+file.name+"_"+fileId+".parquet")
                            .withPartitionPath("ss_sold_date_sk="+partitionId)
                            .withFormat(FileFormat.PARQUET)
                            .withFileSizeInBytes(file.fileSize)
                            .withRecordCount(file.count)
                            .build();
                    tableAppend.appendFile(dataFile);
                }
            }
        }

        tableAppend.commit();
        LOGGER.info("Written snapshot " + table.currentSnapshot());
    }

    private List<FileMetadata> getFiles() {
        List<FileMetadata> files = new ArrayList<>();
        files.add(new FileMetadata("1mb", 1877240L, 25000L));
        files.add(new FileMetadata("10mb", 9035983L, 121077L));
        files.add(new FileMetadata("20mb", 20251148L, 271919L));
        files.add(new FileMetadata("30mb", 35653163L, 874509L));
        files.add(new FileMetadata("50mb", 65430059L, 875846L));
        files.add(new FileMetadata("80mb", 83086932L, 1066582L));
        files.add(new FileMetadata("160mb", 167211734L, 2190815L));
        files.add(new FileMetadata("256mb", 268441678L, 3522388L));
        files.add(new FileMetadata("512mb", 537721359L, 7023189L));
        files.add(new FileMetadata("1024mb", 1002142775L, 13128223L));
        return files;
    }

    private static class FileMetadata {
        private String name;
        private long fileSize;
        private long count;

        public FileMetadata(String name, long fileSize, long count) {
            this.name = name;
            this.fileSize = fileSize;
            this.count = count;
        }
    }







}
