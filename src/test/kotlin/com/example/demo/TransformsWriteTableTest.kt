package com.example.demo

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File

class TransformsWriteTableTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark =
                SparkSession.builder()
                    .appName("TestWriteTable")
                    .master("local")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                    .config("spark.sql.catalog.spark_catalog.warehouse", "build/tmp/iceberg-warehouse")
                    .config("spark.sql.catalog.default", "spark_catalog")
                    .config("spark.ui.enabled", false)
                    .getOrCreate()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            spark.stop()
        }
    }

    @Test
    fun `writeTable writes to iceberg table`() {
        val testFile = File("src/test/resources/sample.csv")
        val df = readFile(spark, testFile.absolutePath)
        val tableName = "test_iceberg_table_write"

        // Create the Iceberg table in the default namespace
        spark.sql("CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, age INT) USING iceberg")

        writeTable(df, tableName)

        val resultDf = spark.table(tableName)
        assertEquals(df.count(), resultDf.count())
    }
}
