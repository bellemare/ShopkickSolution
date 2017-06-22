import org.json4s.JsonAST.JValue

package object schema {
  type SerializedLog = String
  type SerializedRequest = String
  type Request = JValue
  /*
   *  XXX The Kite SDK uses Avro 1.7.5, but avro.Schema became
   *  serializable in Avro 1.8.0. Therefore, we need to serialize
   *  a Schema ourselves for it to be usable in an RDD
   *  
   *  JSON strings are the easiest format to serialize/deserialize because
   *  a Schema can be easily represented by a JSON string
   */
  type SerializedSchema = String
}