/*
 * Copyright (C) 2017-2019 Dremio Corporation. This file is confidential and private property.
 */
package com.dremio.iceberg.testdata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;

public class IcebergTestDataMaker {

    public static void main(String[] args) throws IOException {
        final Path location = Paths.get("/Users/naveenkumar/Desktop/dremio_work/iceberg/table_optimize_1");
        if (Files.exists(location)) {
            Files.walk(location).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete); // Delete if exists
            Files.deleteIfExists(location);
        }

        IcebergTestDataMaker testDataMaker = new IcebergTestDataMaker(location);
        testDataMaker.createSnapshotWithCopies("1mb_0.parquet", 5);
    }

    private static final Logger LOGGER = Logger.getLogger(IcebergTestDataMaker.class.getName());
    private final Path tableLocation;
    private final Path dataFileLocation;

    private PartitionSpec partitionSpec;
    private final Schema schema;

    private final Configuration conf = new Configuration();
    private final Table table;

    public IcebergTestDataMaker(Path tableLocation) throws IOException {
        this.tableLocation = tableLocation;
        this.dataFileLocation = this.tableLocation.resolve("data");

        this.schema = new Schema(
                Types.NestedField.optional(0, "ss_sold_date_sk", Types.LongType.get()),
                Types.NestedField.optional(1, "ss_sold_time_sk", Types.LongType.get()),
                Types.NestedField.optional(2, "ss_item_sk", Types.LongType.get()),
                Types.NestedField.optional(3, "ss_customer_sk", Types.LongType.get()),
                Types.NestedField.optional(4, "ss_cdemo_sk", Types.LongType.get()),
                Types.NestedField.optional(5, "ss_hdemo_sk", Types.LongType.get()),
                Types.NestedField.optional(6, "ss_addr_sk", Types.LongType.get()),
                Types.NestedField.optional(7, "ss_store_sk", Types.LongType.get()),
                Types.NestedField.optional(8, "ss_promo_sk", Types.LongType.get()),
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
        //this.partitionSpec = PartitionSpec.builderFor(schema).withSpecId(0).identity("id").build(); // Un-partitioned

        this.table = new HadoopTables(conf).create(schema, PartitionSpec.unpartitioned(), this.tableLocation.toAbsolutePath().toString());

        Files.createDirectory(dataFileLocation);
        LOGGER.info("Created table " + tableLocation);
    }

    private void createSnapshotWithCopies(String templateFilePathStr, int noOfCopies) throws IOException {
        AppendFiles tableAppend = table.newAppend();
        UUID passId = UUID.randomUUID();
        Path templatePath = Paths.get(templateFilePathStr);
        Preconditions.checkState(Files.exists(templatePath));

        for (int copyId = 0; copyId < 1; copyId++) {
            Path newFilePath = this.dataFileLocation.resolve(String.format("1mb_0.parquet", passId, copyId));
            Files.copy(templatePath, newFilePath);
            DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath(newFilePath.toAbsolutePath().toString())
                    .withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(563L)
                    .withRecordCount(9)
                    .build();
            tableAppend.appendFile(dataFile);
            LOGGER.info("Included data file " + newFilePath);
        }
        tableAppend.commit();
        LOGGER.info("Written snapshot " + table.currentSnapshot());
    }
}
