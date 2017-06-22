package parser

import org.scalatest.FunSuite

class ShopKickSpecificJsonParserUtilTest extends FunSuite {
  val sampleJson =
    """
      {
        "client_app_version": "1.1.1",
        "server_event_timestamp": 123,
        "events": [
          {
            "user_id": 123,
            "device_id": 123,
            "action": 123
          },
          {
            "user_id": 123,
            "event_type": 123,
            "signal_strength": 123.456
          }
        ]
      }
    """

  test("ShopKickSpecificJsonParserUtil.parseAndFlattenJsonEvents successfully flattens the events with proper content.") {

    val expectedMapOne = Map("client_app_version" -> "1.1.1",
      "server_event_timestamp" -> 123,
      "exploded_user_id" -> 123,
      "exploded_device_id" -> 123,
      "exploded_action" -> 123)

    val expectedMapTwo = Map("client_app_version" -> "1.1.1",
      "server_event_timestamp" -> 123,
      "exploded_user_id" -> 123,
      "exploded_event_type" -> 123,
      "exploded_signal_strength" -> 123.456)

    val output = ShopKickSpecificJsonParserUtil.parseAndFlattenJsonEvents(sampleJson)

    val resultOne = output(0).values.asInstanceOf[Map[String, Object]].equals(expectedMapOne)
    if (!resultOne) {
      println("The following are not equal:")
      println(output(0))
      println(expectedMapOne)
    }
    assert(resultOne)

    val resultTwo = output(1).values.asInstanceOf[Map[String, Object]].equals(expectedMapTwo)
    if (!resultTwo) {
      println("The following are not equal:")
      println(output(1))
      println(expectedMapTwo)
    }
    assert(resultTwo)
  }
}
