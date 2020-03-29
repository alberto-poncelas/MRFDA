name := "CreateLuceneData"

version := "1.0"

scalaVersion := "2.12.0"



libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "2.4.2",
	"org.apache.lucene" % "lucene-core" % "5.4.1" from "https://repo1.maven.org/maven2/org/apache/lucene/lucene-core/5.4.1/lucene-core-5.4.1.jar",
	"org.apache.lucene" % "lucene-analyzers-common" % "5.4.1" from "https://repo1.maven.org/maven2/org/apache/lucene/lucene-analyzers-common/5.4.1/lucene-analyzers-common-5.4.1.jar"
)

