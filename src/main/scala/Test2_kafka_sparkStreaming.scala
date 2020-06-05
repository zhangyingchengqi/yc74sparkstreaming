import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._

/*
  kakfa基本操作:   测试环境:伪分布式，配置如下:
    本机一台 zk,端口 2181,使用kafka自带的 zk
    kafka使用伪分布式, 占用三个端口: 9092,9093,9094

   启动与关闭:
        1. 先启 kafka自带的 zk(端口2181): bin/zookeeper-server-start.sh config/zookeeper.properties 1>/dev/null 2>&1 &
        2. 再启动 kafka(端口9092):
              bin/kafka-server-start.sh config/server.properties  &
              bin/kafka-server-start.sh config/server-1.properties  &
              bin/kafka-server-start.sh config/server-2.properties  &

        3. 关闭:  sh kafka-server-stop.sh
                  sh zookeeper-server-stop.sh

   创建主题: bin/kafka-topics.sh --create --zookeeper localhost:2181  --replication-factor 3  --partitions 3 --topic  topic74streaming
   主题列表:  bin/kafka-topics.sh --list --zookeeper localhost:2181
   查看主题中消息详情: bin/kafka-topics.sh --describe --zookeeper localhost:2181    --topic topic74streaming
   发送消息: bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093,localhost:9094 --topic topic74streaming
   消费消息:
     bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093,localhost:9094  --topic   topic74streaming  --from-beginning


     需求: sparkstreaming联接kafka,读取消息，完成单词计数
     注意：当前是一个无状态的操作
     步骤:  1.导入驱动
            2. 编码实现

 */
object Test2_kafka_sparkStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR) //配置日志
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    //当前是consumer端，要反序列化消息
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "streaming74",    //消费者组编号
      "auto.offset.reset" -> "latest",       //消息从哪里开始读取  latest 从头
      "enable.auto.commit" -> (true: java.lang.Boolean)   //消息的位移提交方式
    )
    //要订阅的主题
    val topics = Array("topic74streaming")
    //创建DStream
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)    //     SubscribePattern:主题名由正则表示    , Subscribe:主题名固定   Assign:固定分区
    )
    //    ConsumerRecord
    val lines:DStream[String]=stream.map(record => (   record.value)  )
    val words:DStream[String]=lines.flatMap(   _.split(" "))
    val wordAndOne:DStream[(String,Int)]=words.map(   (_,1) )
    val reduced:DStream[  (String,Int) ]=wordAndOne.reduceByKey( _+_)
    reduced.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
