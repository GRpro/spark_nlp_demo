package com.gr.analytics.text

import java.io.FileInputStream
import java.nio.file.{Files, Path, Paths}

import opennlp.tools.langdetect.{LanguageDetectorME, LanguageDetectorModel}
import opennlp.tools.postag.{POSModel, POSTaggerME}
import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source


object TextAnalytics {

  private implicit val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Spark dataset analyzer")
    .getOrCreate

  def main(args: Array[String]): Unit = {

    val baseDir = Paths.get("./data")
    Files.createDirectories(baseDir)

    val textsZipUri = "http://activewizards.com/content/sc-test-assignment/text-data.zip"
    val textsZipFile = baseDir.resolve("text-data.zip")
    val textsDestDir = baseDir.resolve("text-data")

    val vocabsZipUri = "http://activewizards.com/content/sc-test-assignment/vocabs.zip"
    val vocabsZipFile = baseDir.resolve("vocabs.zip")
    val vocabsDestDir = baseDir.resolve("vocabs")

    val engStopWordsPath = vocabsDestDir.resolve("vocabs").resolve("en-stopwords.txt")
    val deStopWordsPath = vocabsDestDir.resolve("vocabs").resolve("de-stopwords.txt")
    val frStopWordsPath = vocabsDestDir.resolve("vocabs").resolve("fr-stopwords.txt")

    val engTokenModelPath = vocabsDestDir.resolve("vocabs").resolve("en-token.bin")
    val deTokenModelPath = vocabsDestDir.resolve("vocabs").resolve("de-token.bin")
    val frTokenModelPath = vocabsDestDir.resolve("vocabs").resolve("fr-token.bin")

    val engPosTagModelPath = vocabsDestDir.resolve("vocabs").resolve("en-pos-maxent.bin")
    val deuPosTagModelPath = vocabsDestDir.resolve("vocabs").resolve("de-pos-maxent.bin")
    val fraPosTagModelPath = vocabsDestDir.resolve("vocabs").resolve("fr-pos-maxent.bin")

    val engLemmatizerModelPath = vocabsDestDir.resolve("vocabs").resolve("en-lemmatizer.txt")
    val deuLemmatizerModelPath = vocabsDestDir.resolve("vocabs").resolve("de-lemmatizer.txt")
    val fraLemmatizerModelPath = vocabsDestDir.resolve("vocabs").resolve("fr-lemmatizer.txt")

    val languageDetectorModelUri = "http://apache.volia.net/opennlp/models/langdetect/1.8.3/langdetect-183.bin"
    val languageDetectorModelFile = baseDir.resolve("langdetect-183.bin")

    val outputDir = baseDir.resolve("output")
    /* download Dataset */
    Util.loadResource(languageDetectorModelUri, languageDetectorModelFile)
    Util.loadAndUnzip(textsZipUri, textsZipFile, textsDestDir)
    Util.loadAndUnzip(vocabsZipUri, vocabsZipFile, vocabsDestDir)


    def getTokenModelFile(langStr: String): Path = langStr match {
      case "deu" => deTokenModelPath
      case "eng" => engTokenModelPath
      case "fra" => frTokenModelPath
      case unsupported => throw new IllegalArgumentException(s"unsupported language: [$unsupported]")
    }

    def getPosTagModelFile(langStr: String): Path = langStr match {
      case "deu" => deuPosTagModelPath
      case "eng" => engPosTagModelPath
      case "fra" => fraPosTagModelPath
      case unsupported => throw new IllegalArgumentException(s"unsupported language: [$unsupported]")
    }

    def getLammatizerModelFile(langStr: String): Path = langStr match {
      case "deu" => deuLemmatizerModelPath
      case "eng" => engLemmatizerModelPath
      case "fra" => fraLemmatizerModelPath
      case unsupported => throw new IllegalArgumentException(s"unsupported language: [$unsupported]")
    }

    def getStopWordsFile(langStr: String): Path = langStr match {
      case "deu" => deStopWordsPath
      case "eng" => engStopWordsPath
      case "fra" => frStopWordsPath
      case unsupported => throw new IllegalArgumentException(s"unsupported language: [$unsupported]")
    }

    val m = new LanguageDetectorModel(new FileInputStream(languageDetectorModelFile.toFile))
    val langCategorizer = new LanguageDetectorME(m)


    import sparkSession.implicits._

    import scala.collection.JavaConverters._

    val languageTexts: Map[String, Seq[Path]] = Files.list(textsDestDir).iterator().asScala.toList.map(path => {

      // TODO improve performance by detecting language reading first N lines, not whole file
      val text = Source.fromFile(path.toFile).mkString

      // detect language
      val lang = langCategorizer.predictLanguage(text)
      (lang.getLang, path)
    }).groupBy(_._1).mapValues(_.map(_._2))


    val result = languageTexts.map {
      case (lang, files) =>

        val tokenizerModel = new TokenizerModel(getTokenModelFile(lang).toFile)
        val tokenizerModelBroadcast = sparkSession.sparkContext.broadcast(tokenizerModel)

        val stopWords = Source.fromFile(getStopWordsFile(lang).toFile).getLines().toSet
        val stopWordsBroadcast = sparkSession.sparkContext.broadcast(stopWords)

        val posModel = new POSModel(getPosTagModelFile(lang).toFile)
        val posModelBroadcast = sparkSession.sparkContext.broadcast(posModel)

        val lemmasMap: Map[(String, String), String] = Source.fromFile(getLammatizerModelFile(lang).toFile).getLines().map(line => {
          val splitted = line.split("\t")
          val token = splitted(0)
          val lemma = splitted(1)
          val pos = splitted(2)

          ((token, pos), lemma)
        }).toMap
        val lemmasMapBroadcast = sparkSession.sparkContext.broadcast(lemmasMap)


        val DFs = files.map(file => {
          val fileName = file.getFileName.toString

          val textDF = sparkSession.read.text(file.toString)

          // split text into filtered tokens
          val tokenizedDF = textDF.flatMap {
            case Row(text: String) if text.nonEmpty =>
              // TODO probably improve performance by creating TokenizerME per thread
              val tokenazerME = new TokenizerME(tokenizerModelBroadcast.value)
              val tokens = tokenazerME.tokenize(text)

              // filter tokens
              val allowedRegex = "^[a-zA-Z0-9]+.*".r // starts from alphanumeric symbol

              val filteredTokens = tokens.filter(token => {
                allowedRegex.pattern.matcher(token).matches() && !stopWordsBroadcast.value(token)
              })

              val posME = new POSTaggerME(posModelBroadcast.value)
              val posTags = posME.tag(filteredTokens)

              val wordZipTag = filteredTokens zip posTags

              val lemmatizedTokens = wordZipTag.map(entry => {
                val word = entry._1
                val pos = entry._2
                lemmasMapBroadcast.value.getOrElse((word, pos), word)
              })

              Seq(lemmatizedTokens.toSeq)
            case Row(_: String) =>
              Seq.empty
          }.toDF("tokens")
            .select(lit(fileName).as("file"), $"tokens")
            .groupBy("file").agg(collect_list("tokens").as("tokens"))
            .map {
              case Row(file: String, tokens: Seq[Seq[String]]) =>
                (file, tokens.flatten)
            }

          tokenizedDF.show()

          tokenizedDF
        })

        val textsDF = DFs.reduce(_ union _).toDF("filename", "tokens")

        textsDF.show()

        val countVectorizerModel = new CountVectorizer()
          .setInputCol("tokens")
          .setOutputCol("rawFeatures")
          .fit(textsDF)

        val vocabularyBroadcast = sparkSession.sparkContext.broadcast(countVectorizerModel.vocabulary)

        val tokenCountsDF = countVectorizerModel.transform(textsDF)

        val idfModel = new IDF()
          .setInputCol("rawFeatures")
          .setOutputCol("features")
          .fit(tokenCountsDF)

        val tokenIdfDF = idfModel.transform(tokenCountsDF)

        val topWordsDF = tokenIdfDF.select("filename", "features").map(row => {
          val fileName = row.getAs[String]("filename")
          val features = row.getAs[SparseVector]("features")

          val vocabulary = vocabularyBroadcast.value

          // TODO do not sort all elements
          val topTokens = (features.indices zip features.values).sortBy(-_._2).take(30).map(entry => (vocabulary(entry._1), entry._2))

          (fileName, topTokens)
        }).toDF("filename", "keywords").withColumn("language", lit(lang))
          .select(
            $"filename",
            $"language",
            $"keywords".cast(ArrayType(StructType(Seq(StructField("keyword", StringType), StructField("value", DoubleType)))))
          )

        topWordsDF
    }.reduce(_ union _)

    result.show(1000, false)

    result.repartition(1).write.format("json").save(outputDir.toString)
  }

}
