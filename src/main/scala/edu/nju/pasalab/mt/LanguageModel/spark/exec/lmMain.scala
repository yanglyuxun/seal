package edu.nju.pasalab.mt.LanguageModel.spark.exec

import java.io.File

import com.typesafe.config.ConfigFactory
import edu.nju.pasalab.mt.LanguageModel.util.{NGram, Indexing}
import edu.nju.pasalab.util.configRead
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by YWJ on 2015/12/15.
  * Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object lmMain {

  def main(args: Array[String]) {

    /*Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)*/

    val config = ConfigFactory.parseFile(new File(args(0)))
    val ep = configRead.getTrainingSettings(config)

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer","128k")
      .config("spark.kryo.registrator", "edu.nju.pasalab.util.MyRegistrator")
      .config("spark.kryo.registrationRequired", ep.registrationRequired)
      .config("spark.default.parallelism", ep.parallelism)
      .config("spark.memory.offHeap.enabled",ep.isOffHeap)
      .config("spark.memory.offHeap.size", ep.offHeapSize)
      .config("spark.driver.extraJavaOptions", ep.driverExtraJavaOptions)
      .config("spark.executor.extraJavaOptions", ep.executorExtraJavaOptions)
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "true")
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.codec", ep.compressType)
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    configRead.printTrainingSetting(ep)
    configRead.printIntoLog(ep)

    /*val input = args(0)
    val partNum = args(1).toInt
    val N = args(2).toInt
    val splitDataRation = args(3).toDouble
    val sampleNum =  args(4).toInt
    val expectedErrorRate = args(5).toDouble
    val offset = args(6).toInt*/

    // 将所有word替换成id
    val data = NGram.encodeString(spark,
      spark.sparkContext.textFile(ep.lmInputFile, ep.partitionNum).filter(x => x.length > 0),
      ep.lmRootDir)
    /*.map(x => {
    val sp = new StringBuilder(200)
    sp.append("<s> ").append(x).append(" </s>")
    sp.toString()
  })*/
    // 求出 1-gram - N-gram的count，存储
    val grams = NGram.getGrams(data, ep.N).map(_.persist(StorageLevel.MEMORY_ONLY))
    // 求出 每种n-gram内的count和
    val count = NGram.getCount(grams, ep.N)
    for (elem <- count)
      print(elem + "\t")
    println()

    /*val split = data.randomSplit(Array(ep.splitDataRation, 1.0 - ep.splitDataRation), seed = 1000L)
    val (trainData, testData) = (split(0), split(1))
    val testSet = spark.sparkContext.makeRDD(testData.takeSample(false, ep.sampleNum))
    println(trainData.count() + "\n" + testSet.count())
    val indexing = Indexing.createIndex(data, ep.N)
    indexing.foreach(_.persist())*/

    if (ep.GT) GTMain.run(spark, grams, ep) // Good Turing (GT)

    if (ep.GSB) GSBMain.run(spark, grams, count, ep)  // Google StupidbackOff(GSB)

    if (ep.KN) KNMain.run(spark, grams, count, ep) //Kneser-Ney (KN)

    if (ep.MKN) MKNMain.run(spark, grams, count, ep) // Modified Kneser-Ney (MKN)

    grams.map(_.unpersist())
    spark.stop()
  }
}

