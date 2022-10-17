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
        final Path location = Paths.get("/tmp/test4");
        if (Files.exists(location)) {
            Files.walk(location).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete); // Delete if exists
            Files.deleteIfExists(location);
        }

        IcebergTestDataMaker testDataMaker = new IcebergTestDataMaker(location);
        testDataMaker.createSnapshotWithCopies("0_0_0.parquet", 5);
    }

    private static final Logger LOGGER = Logger.getLogger(IcebergTestDataMaker.class.getName());
    private final Path tableLocation;
    private final Path dataFileLocation;

    private final PartitionSpec partitionSpec;
    private final Schema schema;

    private final Configuration conf = new Configuration();
    private final Table table;

    public IcebergTestDataMaker(Path tableLocation) throws IOException {
        this.tableLocation = tableLocation;
        this.dataFileLocation = this.tableLocation.resolve("data");

        this.schema = new Schema(
                Types.NestedField.optional(0, "id", Types.IntegerType.get()),
                Types.NestedField.optional(1, "data", Types.StringType.get())
        );
        this.partitionSpec = PartitionSpec.builderFor(schema).withSpecId(0).identity("id").build(); // Un-partitioned

        this.table = new HadoopTables(conf).create(schema, partitionSpec, this.tableLocation.toAbsolutePath().toString());

        Files.createDirectory(dataFileLocation);
        LOGGER.info("Created table " + tableLocation);
    }

    private void createSnapshotWithCopies(String templateFilePathStr, int noOfCopies) throws IOException {
        AppendFiles tableAppend = table.newAppend();
        UUID passId = UUID.randomUUID();
        Path templatePath = Paths.get(templateFilePathStr);
        Preconditions.checkState(Files.exists(templatePath));

        for (int copyId = 0; copyId < noOfCopies; copyId++) {
            Path newFilePath = this.dataFileLocation.resolve(String.format("%s_%d.parquet", passId, copyId));
            Files.copy(templatePath, newFilePath);
            DataFile dataFile = DataFiles.builder(partitionSpec)
                    .withPath(newFilePath.toAbsolutePath().toString())
                    //.withPartitionPath("id=2/name=jill")
                    .withFormat(FileFormat.PARQUET).withFileSizeInBytes(563L).withRecordCount(9).build();
            tableAppend.appendFile(dataFile);
            LOGGER.info("Included data file " + newFilePath);
        }
        tableAppend.commit();
        LOGGER.info("Written snapshot " + table.currentSnapshot());
    }
}
