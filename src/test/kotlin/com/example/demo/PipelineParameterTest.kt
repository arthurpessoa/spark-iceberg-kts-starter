package com.example.demo

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class PipelineParameterTest {
    @Test
    fun `getParameter returns value when present`() {
        val args = arrayOf("--file=foo.csv", "--other=bar")
        val value = args.getParameter("--file=")
        assertEquals("foo.csv", value)
    }

    @Test
    fun `getParameter throws when key not present`() {
        val args = arrayOf("--other=bar")
        assertThrows(IllegalStateException::class.java) {
            args.getParameter("--file=")
        }
    }

    @Test
    fun `getParameter returns empty string if key present but no value`() {
        val args = arrayOf("--file=", "--other=bar")
        val value = args.getParameter("--file=")
        assertEquals("", value)
    }
}
