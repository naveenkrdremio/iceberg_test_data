package com.dremio.parquet;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import com.google.common.collect.ImmutableList;

public class ParquetDataFileWriter {
    public static void writeToParquet(String parquetFilePath, int noOfColumns, int noOfPartitionColumns, int noOfRecords) throws IOException {


        Types.NestedField []cols = new Types.NestedField[noOfColumns+noOfPartitionColumns];
        for (int i = 0; i < noOfColumns; i++) {
            cols[i] = Types.NestedField.optional(i, "col_"+i, Types.StringType.get());
        }
        for (int i = noOfColumns; i < noOfColumns+noOfPartitionColumns; i++) {
            cols[i] = Types.NestedField.optional(i, "partition_"+i, Types.StringType.get());
        }
        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(cols);
        GenericRecord record = GenericRecord.create(schema);


        ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
        for (int j = 0; j < noOfRecords; j++) {
            Map<String, Object> records = new HashMap<>();
            for (int i = 0; i < noOfColumns; i++) {
                records.put("col_"+i, "value_"+ UUID.randomUUID());
            }
            for (int i = noOfColumns; i < noOfColumns+noOfPartitionColumns; i++) {
                Random random = new Random();
                records.put("partition_"+i, "partition_value_"+random.nextInt(noOfPartitionColumns*100) );
            }
            builder.add(record.copy(records));
        }

        HadoopOutputFile.fromPath(new Path(parquetFilePath), new Configuration());
        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(HadoopOutputFile.fromPath(new Path(parquetFilePath), new Configuration()))
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();
        try {
            for (GenericRecord entry : builder.build()) {
                dataWriter.write(entry);
            }
        } finally {
            dataWriter.close();
        }
    }

    public static void main(String[] args) {
        try {
            for (int i = 0; i < 100; i++) {
                writeToParquet(i+"_data_file.parquet", 600,5,10000);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
