package com.example.demo

import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class SchemaConvertTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark =
                SparkSession.builder()
                    .appName("TestConvertSchemaWithRename")
                    .master("local[1]")
                    .getOrCreate()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            spark.stop()
        }
    }

    @Test
    fun `convertSchemaWithRename renames and casts columns correctly`() {
        val sourceSchema =
            StructType(
                arrayOf(
                    StructField("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField("full_name", DataTypes.StringType, false, Metadata.empty()),
                    StructField("years", DataTypes.StringType, false, Metadata.empty()),
                ),
            )
        val targetSchema =
            StructType(
                arrayOf(
                    StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                ),
            )
        val data = listOf(RowFactory.create("1", "Alice", "30"))
        val df = spark.createDataFrame(data, sourceSchema)
        val result = convertSchemaWithRename(df, sourceSchema, targetSchema)
        val row = result.first()
        assertEquals(1, row.getAs<Int>("id"))
        assertEquals("Alice", row.getAs<String>("name"))
        assertEquals(30, row.getAs<Int>("age"))
    }

    @Test
    fun `convertSchemaWithRename throws exception for incompatible schemas`() {
        val sourceSchema =
            StructType(
                arrayOf(
                    StructField("id", DataTypes.StringType, false, Metadata.empty()),
                    StructField("full_name", DataTypes.StringType, false, Metadata.empty()),
                ),
            )
        val targetSchema =
            StructType(
                arrayOf(
                    StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                    StructField("name", DataTypes.StringType, false, Metadata.empty()),
                    StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                ),
            )
        val data = listOf(RowFactory.create("1", "Alice"))
        val df = spark.createDataFrame(data, sourceSchema)
        assertThrows<IllegalArgumentException> {
            convertSchemaWithRename(df, sourceSchema, targetSchema)
        }
    }
}
