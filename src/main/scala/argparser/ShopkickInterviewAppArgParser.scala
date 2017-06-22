package argparser

object ShopkickInterviewAppArgParser {
  val DEFAULT_UNSET = "unset"
  case class CliArgs( inputUri: String = DEFAULT_UNSET,
                      outputUri: String = DEFAULT_UNSET)


  val parser = new scopt.OptionParser[CliArgs]("scopt") {
    head("ShopKick Interview Application ")
    help("help").text("Read data from s3 a.wishabi.com")

    opt[String]("inputUri")
      .text("The URI from which to read the input json files. NOTE: Assumes each file has one complete json entry per line.")
      .required()
      .action { (x, c) =>
      c.copy(inputUri = x)
    }

    opt[String]("outputUri")
      .text("The URI to which the output is written. Output format is Parquet")
      .required()
      .action { (x, c) =>
      c.copy(outputUri = x)
    }
  }

  def parseArgs(args: Array[String]) : ShopkickInterviewAppArgParser.CliArgs =
    parser
      .parse(args, CliArgs())
      .getOrElse(throw new Exception("Arg parser unable to initialize! Fatal Error"))
}