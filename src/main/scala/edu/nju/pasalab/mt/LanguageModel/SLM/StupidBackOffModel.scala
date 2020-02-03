package edu.nju.pasalab.mt.LanguageModel.SLM

import breeze.util.BloomFilter
import edu.nju.pasalab.mt.LanguageModel.util.NGram
import edu.nju.pasalab.mt.util.{CommonFileOperations, SntToCooc}
import edu.nju.pasalab.util.ExtractionParameters
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


import edu.nju.pasalab.util.gramPartitioner
import edu.nju.pasalab.util.math._

/**
  * This class used Google Stupid BackOff Smoothing to build a n-gram Language Model
  * * for special :
  * 1-gram we add three special token : <s> </s> <unk>, we mark:
  * 0 <s> backoff
  * p </s> 0
  * p <unk> 0
  * Created by wuyan on 2016/3/22.
  */
class StupidBackOffModel extends Serializable{
  var N = 0
  val alpha = 0.4
  var prob : Option[Array[RDD[(Long, Double)]]] = None
  var bloomFilter :Option[Array[BloomFilter[Long]]] = None

  /**
    * construction function for Kneser-Ney Model
    *
    * @param sc is SparkContext
    * @param trainData is the trainSet
    * @param ep parameters
    */
  //index: Array[RDD[(String, Long)]]
  def this(sc : SparkSession, trainData : Array[RDD[(String, Int)]],
           count : Array[Long], ep: ExtractionParameters) {
    this()
    this.N = ep.N
    this.set(sc, trainData, count, ep)
    //this.setBloomFilter(this.prob.get, ep.expectedErrorRate)
  }


  /**
    * @param sc is SparkContext
    * @param grams is the trainSet
    * @param ep parameters
    */
  def set(sc : SparkSession, grams : Array[RDD[(String, Int)]], count : Array[Long],ep: ExtractionParameters): Unit ={
    // trainData: [1-gram, ... , n-gram]，每个gram是[(gram1, cnt1),(gram2,cnt2),...]
    // count: [sum(cnt) of 1-gram, ..., sum(cnt) of n-gram]
    CommonFileOperations.deleteIfExists(ep.lmRootDir + "/gsb")
    val dictFile = sc.sparkContext.textFile(SntToCooc.getLMDictPath(ep.lmRootDir)).filter(_.length > 0).map(line => {
      val sp = line.trim.split("\\s+")
      (sp(0), sp(1).toInt)
    }) // [(word, id), ...]

    val dict : Int2ObjectOpenHashMap[String] = new Int2ObjectOpenHashMap[String]
    for (elem <- dictFile.collect()) {
      dict.put(elem._2, elem._1)
    }
    val idBroad = sc.sparkContext.broadcast(dict) // {id:word, ...}

    for (i <- 0 until N) { // i: n=i+1 in n-gram

      if (i == 0) { // 1-gram
        val gram = grams(i).repartitionAndSortWithinPartitions(new HashPartitioner(ep.partitionNum))

        val gsb = gram.mapPartitions(part => {
          val id = idBroad.value
          part.map(elem => { // elem: [(gram1, cnt1), ...]
            val split = elem._1.trim.split("\\s+") // [gram1.1, gram1.2, ...]
            val sb = new StringBuilder(128)
            val tmp = split(0).toInt // first word

            if (tmp == 0) { // "<s>"
              sb.append(0).append("\t").append("<s>").append("\t").append(Math.log10(alpha)) // "0  <s>  log10(0.4)"
            } else {
              sb.append(basic.Divide(elem._2, count(i))).append("\t").append(id.get(tmp.toInt))
              if (tmp == 1) {
                sb.append("\t").append(0)  // "log10(cnt)-log10(sum(cnt))  </s>  0"
              } else{
                sb.append("\t").append(Math.log10(alpha))  // "log10(cnt)-log10(sum(cnt))  *word*  log10(0.4)"
              }
            }
            sb.toString()
          })
        })
        gsb.union(
          sc.sparkContext.makeRDD(Array(basic.Divide(1.0, count(i)) + "\t" + "unk\t 0.0")) // "-log10(sum(cnt))  unk  0.0"
        ).repartition(ep.partitionNum).saveAsTextFile(SntToCooc.getGSBGramPath(ep.lmRootDir, i+1))

        /*PROB(i) = grams.map(x => (x._1, Divide(x._2, count)))
          .join(indexing)
          .map(elem => (elem._2._2, elem._2._1))*/
      } else { // if 2-gram, 3-gram, ...

        val gram = simpleMap(grams(i).repartitionAndSortWithinPartitions(new gramPartitioner(ep.partitionNum)))
        // [(condition, (full_gram, cnt)), ...]

        val gsb = gram.join(
          gram.map(elem => (elem._1, elem._2._2)).reduceByKey(_+_) // counts of the conditions
        ).mapPartitions(part => {
          val id = idBroad.value
          part.map(elem =>{ // (condition, ((full_gram, cnt), cnt_of_condition))
            val sp = new StringBuilder(128)
            val split = elem._2._1._1.trim.split("\\s+") // split of full gram
            sp.append(basic.Divide(elem._2._1._2, elem._2._2)).append("\t").append(id.get(split(0).toInt))
            for (i <- 1 until split.length)
              sp.append(" ").append(id.get(split(i).toInt))
            // "log10(cnt)-log10(cnt_of_condition)  w1 w2 w3 ..."

            if (i != 4) {
              sp.append("\t").append(Math.log10(alpha)) // "log10(cnt)-log10(cnt_of_condition)  w1 w2 w3 ...  log10(0.4)"
            }
            sp.toString()
          })
        })
        gsb.saveAsTextFile(SntToCooc.getGSBGramPath(ep.lmRootDir, i+1))
       /* PROB(i) = prop
          .join(prop.map(elem => (elem._1, elem._2._2)).reduceByKey(_+_))
          .map(x => (x._2._1._1, Divide(x._2._1._2, x._2._2)))
          .join(indexing)
          .map(elem => (elem._2._2, elem._2._1))*/
      }
    }
    //prob = Some(PROB)
  }

  def simpleMap(data : RDD[(String, Int)]) : RDD[(String, (String, Int))] ={
    val prop = data.map(elem => {
      val tmp = elem._1.substring(0, elem._1.lastIndexOf(" "))
      (tmp, (elem._1, elem._2))
    })
    prop
  }

  def setBloomFilter(arr : Array[RDD[(Long, Double)]], p : Double): Unit = {
    val filter : Array[BloomFilter[Long]] = new Array[BloomFilter[Long]](N)
    for (i <- 0 until N) {
      val set = arr(i).map(x => x._1)
      val (m, k) = BloomFilter.optimalSize(set.count(), p)
      val bloom = new BloomFilter[Long](m ,k, new java.util.BitSet)
      set.collect().map(x => bloom.+=(x))
      filter(i) = bloom
    }
    //bloomFilter = Some(filter)
  }
}
