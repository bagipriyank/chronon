package ai.chronon.online.serde

import ai.chronon.api.{BooleanType, Constants, LongType, StructField, StructType}
import org.slf4j.LoggerFactory

/** Format-agnostic mapping of a Debezium CDC envelope to Chronon Mutation(s).
  *
  * Debezium op semantics:
  *   "c" (create) / "r" (read/snapshot): before=null, after populated
  *   "u" (update):                        both before and after populated
  *   "d" (delete):                        before populated, after=null
  *   anything else (e.g. "t" truncate):   no-op — logged and skipped
  *
  * Output rows have the inner record fields followed by two mutation columns appended at the tail:
  *   [...inner_fields..., mutation_ts: Long, is_before: Boolean]
  */
object DebeziumMutationMapper {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  val DebeziumKey = "debezium"

  /** Builds the output Chronon schema: inner record fields + mutation_ts + is_before. */
  def buildOutputSchema(innerSchema: StructType): StructType =
    StructType(
      innerSchema.name,
      innerSchema.fields :+ StructField(Constants.MutationTimeColumn, LongType) :+ StructField(Constants.ReversalColumn,
                                                                                               BooleanType)
    )

  /** Returns a new array with mutation_ts and is_before appended. */
  def appendMutationColumns(row: Array[Any], mutationTs: java.lang.Long, isBefore: Boolean): Array[Any] = {
    val result = new Array[Any](row.length + 2)
    System.arraycopy(row, 0, result, 0, row.length)
    result(row.length) = mutationTs
    result(row.length + 1) = isBefore: java.lang.Boolean
    result
  }

  /** Maps a parsed Debezium envelope to a Chronon Mutation. */
  def toMutation(
      op: String,
      beforeRow: Array[Any],
      afterRow: Array[Any],
      mutationTs: java.lang.Long,
      schema: StructType
  ): Mutation =
    op match {
      case "c" | "r" =>
        if (afterRow == null) {
          logger.warn("Skipping malformed Debezium op='{}': after is null", op)
          Mutation(schema, null, null)
        } else {
          Mutation(schema, null, appendMutationColumns(afterRow, mutationTs, isBefore = false))
        }

      case "d" =>
        if (beforeRow == null) {
          logger.warn("Skipping malformed Debezium op='d': before is null")
          Mutation(schema, null, null)
        } else {
          Mutation(schema, appendMutationColumns(beforeRow, mutationTs, isBefore = true), null)
        }

      case "u" =>
        if (beforeRow == null || afterRow == null) {
          logger.warn("Skipping malformed Debezium op='u': before={}, after={}", beforeRow == null, afterRow == null)
          Mutation(schema, null, null)
        } else {
          Mutation(
            schema,
            appendMutationColumns(beforeRow, mutationTs, isBefore = true),
            appendMutationColumns(afterRow, mutationTs, isBefore = false)
          )
        }

      case other =>
        // Includes "t" (truncate) and any future op values we don't handle.
        // Emit a no-op Mutation — Chronon's Flink DeserializationSchema produces no rows for it.
        logger.warn("Skipping unsupported Debezium op: '{}'", other)
        Mutation(schema, null, null)
    }
}
