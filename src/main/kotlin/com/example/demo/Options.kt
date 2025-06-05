package com.example.demo

/** * Data class representing the options for the pipeline.
 *
 * @property topic The Kafka topic to read from.
 * @property table The Iceberg table to write to.
 * @property bootstrapServers The Kafka bootstrap servers.
 * @property inputSchema The Avro schema for the input data.
 * @property outputSchema The Avro schema for the output data.
 * @property checkpointLocation The location for Spark's checkpointing.
 */
data class Options(
    val topic: String,
    val table: String,
    val bootstrapServers: String,
    val inputSchema: String,
    val outputSchema: String,
    val checkpointLocation: String,
)

/**
 * Builds the options for the pipeline from command line arguments.
 *
 * @param args The command line arguments.
 * @return An Options object containing the parsed parameters.
 * @throws IllegalStateException if any required parameter is not found.
 */
fun buildOptionsFromArgs(args: Array<String>): Options {
    val topic = args.getParameter("--topic=")
    val table = args.getParameter("--table=")
    val bootstrapServers = args.getParameter("--bootstrap-servers=")
    val inputSchema = args.getParameter("--input-schema=")
    val outputSchema = args.getParameter("--output-schema=")
    val checkpointLocation = args.getParameter("--checkpoint-location=")

    return Options(
        topic = topic,
        table = table,
        bootstrapServers = bootstrapServers,
        inputSchema = inputSchema,
        outputSchema = outputSchema,
        checkpointLocation = checkpointLocation,
    )
}

/**
 * Retrieves a parameter value from the command line arguments.
 *
 * @param key The key of the parameter to retrieve.
 * @return The value of the parameter.
 * @throws IllegalStateException if the parameter is not found.
 */
fun Array<String>.getParameter(key: String): String =
    checkNotNull(this.getOptionParameter(key)) { "Parameter '$key' not found in arguments." }

/**
 * Retrieves a parameter value from the command line arguments, returning null if not found.
 *
 * @param key The key of the parameter to retrieve.
 * @return The value of the parameter, or null if not found.
 */
fun Array<String>.getOptionParameter(key: String): String? = this.find { it.startsWith(key) }?.substringAfter(key)
