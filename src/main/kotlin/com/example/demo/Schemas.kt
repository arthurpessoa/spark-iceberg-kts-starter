package com.example.demo

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object Schemas {
    val csvSchema: StructType =
        StructType(
            arrayOf(
                StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                StructField("name", DataTypes.StringType, false, Metadata.empty()),
                StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
            ),
        )
    val tableSchema: StructType =
        StructType(
            arrayOf(
                StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                StructField("name", DataTypes.StringType, false, Metadata.empty()),
                StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
            ),
        )
    val fileControlSchema: StructType =
        StructType(
            arrayOf(
                StructField("reference_code", DataTypes.StringType, false, Metadata.empty()),
                StructField("file_name", DataTypes.StringType, false, Metadata.empty()),
                StructField("received_at", DataTypes.TimestampType, false, Metadata.empty()),
            ),
        )
}

fun convertSchemaWithRename(
    df: Dataset<Row>,
    sourceSchema: StructType,
    targetSchema: StructType,
): Dataset<Row> {
    require(sourceSchema.fields().size == targetSchema.fields().size) { "Schemas must have the same number of fields" }
    val columns =
        targetSchema.fields().mapIndexed { idx, targetField ->
            val sourceField = sourceSchema.fields()[idx]
            col(sourceField.name()).cast(targetField.dataType()).`as`(targetField.name())
        }
    return df.select(*columns.toTypedArray())
}
