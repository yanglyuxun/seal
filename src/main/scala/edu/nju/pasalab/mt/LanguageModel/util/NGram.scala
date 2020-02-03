package edu.nju.pasalab.mt.LanguageModel.util

import java.io.{OutputStreamWriter, BufferedWriter, PrintWriter}

import edu.nju.pasalab.mt.util.{SntToCooc, CommonFileOperations}
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by YWJ on 2015/12/28.
  *  Copyright (c) 2016 NJU PASA Lab All rights reserved.
 */
object NGram {
  /**
   * @param data is the Set of input
   * @param n   is the value of n-gram
   * @return    n-gram
   */
  def get_NGram(data : RDD[String], n : Int ): RDD[(String, Int)] = {
    // 取出所有n-gram, 然后合并为count输出
    val result = data.mapPartitions(part => {
      part.map(elem => {
        val split = elem.split(" ")
        val arr : ArrayBuffer[(String, Int)] = new ArrayBuffer[(String, Int)]()
        var i = 0
        var j = i + n - 1
        while(((i + n - 1) < split.length) && (j < split.length)) {
          val tmp = new StringBuilder(128)
          tmp.append(split(i))
          for (index <- i+1 until j+1)
            tmp.append(" ").append(split(index))
          arr.append((tmp.toString(), 1))
          tmp.clear()
          i += 1
          j += 1
        }
        arr
      })
    }).flatMap(x => x).reduceByKey(_+_)
    result
  }

  def getGrams(trainData : RDD[String], N : Int): Array[RDD[(String, Int)]] = {
    // [1-gram, 2-gram, ... , N-gram] 注意每一个元素都是count
    val grams = new Array[RDD[(String, Int)]](N) //
    for (i <- 0 until N) {
      grams(i) = get_NGram(trainData, i+1)
    }
    grams
  }

  def getCount(grams :Array[RDD[(String, Int)]], N: Int):Array[Long] = {
    // 每种gram的总数
    val arr = new Array[Long](N)
    for (i <- 0 until N) {
//      arr(i) = grams(i).count()
      arr(i) = grams(i).map(_._2.toLong).reduce(_+ _)
    }
    arr
  }

  /**
    * Special Token : <s> </s> unk : we given id : 0 1 2
    *
    * @param data training data
    * @param lmRootDir language model working directory
    * @return
    */
  def encodeString(sc : SparkSession, data : RDD[String], lmRootDir : String) : RDD[String] = {
    println("Build Dict & encoding line . ")
    // 词频统计，降序排列
    val sorted = data.flatMap(line => line.trim.split("\\s+")) // 分词，行转列
      .map(x => (x, 1))
      .reduceByKey(_+_) // count
      .collect().sortWith(_._2 > _._2) // sort desc

    CommonFileOperations.deleteIfExists(SntToCooc.getLMDictPath(lmRootDir))

    // 编码dict
    val dict : Object2IntOpenHashMap[String] = new Object2IntOpenHashMap()
    // hdfs存储对象，格式：词+" " + cnt
    val dictFile : PrintWriter = new PrintWriter(new BufferedWriter(
      new OutputStreamWriter(CommonFileOperations.openFileForWrite(SntToCooc.getLMDictPath(lmRootDir), true)), 129 * 1024 * 1024))

    dict.put("<s>", 0) // start
    dict.put("</s>", 1)  // end
    dict.put("unk", 2) // unknown

    dictFile.println("<s> " + 0)
    dictFile.println("</s> " + 1)
    dictFile.println("unk " + 2)

    var id = 3
    for(elem <- sorted) {
      dict.put(elem._1, id)
      dictFile.println(elem._1 + " " + id)
      id += 1
    }
    dictFile.close()

    // 广播dict
    val idBroadcast = sc.sparkContext.broadcast(dict)
    val res = data.mapPartitions(part => { // 分区计算
      val id = idBroadcast.value // id: Object2IntOpenHashMap[String] = dict
      part.map(elem => {
        val sb = new StringBuilder(128)
        val split = elem.trim.split("\\s+")
        sb.append(0) // start
        for (elem <- split) {
          sb.append(" ").append(id.getInt(elem))
        }
        sb.append(" ").append(1) // end
        sb.toString()
      })
    })
    res
  }

}