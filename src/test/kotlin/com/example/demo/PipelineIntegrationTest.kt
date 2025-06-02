package com.example.demo

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

class PipelineIntegrationTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark =
                SparkSession.builder()
                    .appName("TestPipelineIntegration")
                    .master("local[1]")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
                    .config("spark.sql.catalog.spark_catalog.warehouse", "build/tmp/iceberg-warehouse")
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
    fun `pipeline runs end-to-end`() {
        // Prepare
        val testFile = "src/test/resources/sample.csv"
        val tableName = "test_iceberg_table_pipeline"
        spark.sql("CREATE TABLE IF NOT EXISTS $tableName (id INT, name STRING, age INT) USING iceberg")

        // Act
        pipeline(spark, arrayOf("--file=$testFile", "--table=$tableName"))

        // Assert
        val resultDf = spark.table(tableName)
        assert(resultDf.count() > 0) { "Table should have data" }
    }
}
