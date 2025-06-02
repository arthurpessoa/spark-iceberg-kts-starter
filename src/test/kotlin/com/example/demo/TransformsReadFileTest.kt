package com.example.demo

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.io.File

class TransformsReadFileTest {
    companion object {
        private lateinit var spark: SparkSession

        @JvmStatic
        @BeforeAll
        fun setup() {
            spark = SparkSession.builder().appName("TestReadFile").master("local[1]").getOrCreate()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            spark.stop()
        }
    }

    @Test
    fun `readFile reads csv correctly`() {
        val testFile = File("src/test/resources/sample.csv")
        val df = readFile(spark, testFile.absolutePath)
        assertEquals(3, df.count())
        val firstRow = df.first()
        assertEquals(1, firstRow.getAs("id"))
        assertEquals("Alice", firstRow.getAs<String>("name"))
        assertEquals(30, firstRow.getAs("age"))
    }
}
