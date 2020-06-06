package Test6_window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.storage.StorageLevel

/*
需求:   批处理时间间隔1 秒, 每10秒统计前30秒各单词累计出现的次数
 */
object TextFileStream {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    //创建一个本地模式的streamingContext,设定master节点工作线程数为2, 以5秒作为批处理时间间隔. 
    val conf = new SparkConf().setMaster("local[*]").setAppName("WindowWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // checkpoint作为容错的设计，基本思路是把当前运行的状态，保存在容错的存储系统中(一般是hdfs)。对于容错的处理，肯定是围绕作业紧密相关的，保存内容包括元数据和数据两部分。
    ssc.checkpoint("./chpoint")  // window操作与有状态的操作
    //指定数据源为hdfs
    val lines = ssc.socketTextStream("localhost",9999, StorageLevel.MEMORY_ONLY_SER);
    val words = lines.flatMap(x => x.split(" "))
    //TODO: 采用reduceByKeyAndWindow操作进行叠加处理，窗口时间间隔与滑动时间间隔
    // 每10秒统计前30秒各单词累计出现的次数
    val wordCounts = words.map(word => (word, 1)).reduceByKeyAndWindow((a:Int,b:Int)=>a+b, Seconds(30)  , Seconds( 10) )
    //println("=====开始打印结果=====")
    //wordCounts.print(  20   )
    //println("=====结束打印=====")
    printValues(wordCounts)
    ssc.start()
    ssc.awaitTermination()
  }
  //定义一个打印函数，打印RDD中所有的元素
  def printValues(stream: DStream[(String, Int)]) {     //    DStream -> n个RDD组成  -> 一个RDD由n 条记录组成  -》一条记录由  (String, Int) 组成
    stream.foreachRDD(foreachFunc) //   不要用foreach()  ->  foreachRDD
    def foreachFunc = (rdd: RDD[(String, Int)]) => {
      val array = rdd.collect()   //采集 worker端的结果传到driver端.
      println("=====begin to show results =====")
      for (res <- array) {
        println(res)
      }
      println("=====ending  show results=====")
    }
  }
}