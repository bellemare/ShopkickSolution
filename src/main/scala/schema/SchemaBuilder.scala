package schema

import java.io.{ByteArrayInputStream, InputStream}
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.kitesdk.data.spi.{JsonUtil, SchemaUtil}

import scala.util.Try

/**
 * Builds Avro schemas by inferring the schema from the underlying data.
 *
 * @param namespace Namespace given to the created Avro record.
 */
class SchemaBuilder(namespace: String) extends Serializable {

  /**
   * Infer request schemas - one per event type - based on the given data
   * @param requests An RDD containing the parsed JValues.
   * @return Avro SerializedSchema, represented as a String
   */
  def inferFrom(requests: RDD[JValue]): SerializedSchema = {
    requests
      .map(inferSchema)
      .reduce(mergeSchemas)
      .get
  }

  /**
   * Infer the Avro Schema for the given request data
   * @param request The individual JValue request containing the underlying event data.
   * @return A Try with the serialized schema wrapped inside.
   */
  protected[schema] def inferSchema(request: JValue): Try[SerializedSchema] = {
    for {
      jsonStream <- Try(stream(compact(request)))
      schema <- Try(JsonUtil.inferSchema(jsonStream, s"$namespace", 1))
    } yield serializeSchema(schema)
  }

  /**
   * helper function to turn a String into an InputStream
   * @param s String to turn into Stream
   * @return ByteArrayInputStream as InputStream
   */
  protected def stream(s: String): InputStream = new ByteArrayInputStream(s.getBytes("UTF-8"))

  /**
   * Merge the two schemas such that the resulting schema is a more general form of the original schemas.
   * i.e. if request X is valid under schema A, and request Y is valid under schema B, then X and Y will be valid in merge(A, B)
   *
   * @param attemptedA Schema A to merge
   * @param attemptedB Schema B to merge
   * @return Merged Schema
   */
  protected[schema] def mergeSchemas(attemptedA: Try[SerializedSchema], attemptedB: Try[SerializedSchema]): Try[SerializedSchema] = {
    for {
      a <- attemptedA
      b <- attemptedB
      schemaA = deserializeSchema(a)
      schemaB = deserializeSchema(b)
      merged <- Try(SchemaUtil.merge(schemaA, schemaB))
    } yield serializeSchema(merged)
  }

  //Schema objects aren't serializable, but Strings are.
  protected[schema] def serializeSchema(schema: Schema): SerializedSchema = schema.toString
  protected[schema] def deserializeSchema(schema: SerializedSchema): Schema = Schema.parse(schema)
}