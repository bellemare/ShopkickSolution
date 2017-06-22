name := "ShopKickInterview"

version := "20170621.0"

scalaVersion := "2.11.8"

compileOrder := CompileOrder.JavaThenScala

resolvers ++= Seq(
  Resolver.mavenLocal,
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "A second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Twitter Maven Repo" at "http://maven.twttr.com/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

//Note: Must use parquet-avro 1.8.2 with Spark 2.1 or higher, as there are some dependency issues to resolve otherwise.
//If using 1.6.x, I believe you can use parquet-avro 1.7, but will need to rewrite code to use older ways of creating the readers / writers.
libraryDependencies ++= Seq(
  //Parameter Input
  "com.github.scopt"  %% "scopt"           % "3.5.0",
  "org.apache.spark"  %% "spark-core"      % "2.1.1",
  "org.apache.spark"  %% "spark-sql"       % "2.1.1",
  "org.scalatest"     %% "scalatest"       % "3.+",
  //"org.log4s"         %% "log4s"           % "1.3.5",
  "org.json4s"        %% "json4s-jackson"   % "3.2.11", //Avoids issues with json4s?
  "org.apache.parquet" % "parquet-avro" % "1.8.2",
  "org.kitesdk"       %  "kite-data-core"   % "1.1.0",
  //Testing Avro stuff
  "com.databricks" %% "spark-avro" % "3.2.0",
  "org.slf4j"         %  "slf4j-api"       % "1.+",
  "org.clapper" %% "grizzled-slf4j" % "1.+"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
  case "about.html"  => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

parallelExecution in Test := false
