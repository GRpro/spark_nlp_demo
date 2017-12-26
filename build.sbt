
name := "spark_dataset_analyzer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val sparkVersion = "2.1.0"
  Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
  )
}



// https://mvnrepository.com/artifact/edu.stanford.nlp/stanford-corenlp
//libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0"

libraryDependencies += "org.apache.opennlp" % "opennlp-tools" % "1.8.4"

