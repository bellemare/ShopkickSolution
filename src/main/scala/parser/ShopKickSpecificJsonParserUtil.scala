package parser

import org.codehaus.jackson.map.ObjectMapper
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._ //Used to convert Scala Map to Java Map for ObjectMapper

object ShopKickSpecificJsonParserUtil {
  val EXPLODED = "exploded_"
  val EVENTS = "events"

  def parseAndFlattenJsonEvents(json: String): List[JValue] = {
    val result = parse(json)
    val jsonMap = result.values.asInstanceOf[Map[String, Object]]

    //filter out the events key - we will add the individual exploded events later.
    val elems = jsonMap.filter(x => x._1 != EVENTS)

    val events = result \ EVENTS

    val mapOfEventData = events.children
      .map(x => {
      val explodedMap = x.values.asInstanceOf[Map[String, Object]].map(x => (EXPLODED + x._1, x._2))
      val jsonString = new ObjectMapper().writeValueAsString((explodedMap ++ elems).asJava)
      parse(jsonString)
    })
    mapOfEventData
  }
}
