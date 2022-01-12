import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.common.serialization.StringDeserializer

object FlinkSteaming {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    StreamExecutionEnvironment.createLocalEnvironment(1)
//    StreamExecutionEnvironment.createRemoteEnvironment("172.168.72.64",4044,"")
    env.setParallelism(4)
//    val data: DataStream[SendsoRendsourceTest] = env.fromCollection(List(
//      SendsoRendsourceTest("ceshi1",1547718199,123),
//      SendsoRendsourceTest("ceshi2",1287319287,100),
//      SendsoRendsourceTest("ceshi2",1287319287,99),
//      SendsoRendsourceTest("ceshi1",1547718200,123),
//      SendsoRendsourceTest("ceshi1",1547718201,123),
//      SendsoRendsourceTest("ceshi1",1547718202,123)
//    ))
//    data.print("data")
//    val data: DataStream[String] = env.readTextFile("E:\\java_home_work\\IdeaProjects\\zbs_learn\\zbs_spring\\src\\main\\resources\\Tess_data")
//    env.socketTextStream("",21)
// v从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.serers","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("auto.offset.reset","latest")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    val data: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("ceshi",new SimpleStringSchema(),properties))
    data.print("data")
    data.flatMap(_.split(" ")).map((_,1)).slotSharingGroup("1").disableChaining()
      .filter(_._1.nonEmpty).keyBy(0).sum(1).startNewChain()
    data.slotSharingGroup("1")
    env.execute("source test job")
    val data2: DataStream[String] = data.split(x => {
      if (x.isEmpty) {
        List("high")
      }
      else
        List("low")
    }).select("high")


  }

}
//数据的输入形式是集合
case class SendsoRendsourceTest(id:String,timestap:Long,var  rmperatur:Double)
//从文件中读取数据

class MyidSelect() extends KeySelector[SendsoRendsourceTest,String]{
  override def getKey(in: SendsoRendsourceTest): String = in.id
}

object sourceDemo{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream5: DataStream[SendsoRendsourceTest] = env.addSource(new MysensorSource())
    val value2: DataStream[SendsoRendsourceTest] = stream5.keyBy(new MyidSelect())
      .reduce(
        (a: SendsoRendsourceTest, b: SendsoRendsourceTest) => {
          b.rmperatur = a.rmperatur + b.rmperatur
          b
        }
      )
    value2.print()
    env.execute("ceshisource")
  }
}
//实现一个自定义的sourceFunction,自动生成测试数据
class MysensorSource extends RichSourceFunction[SendsoRendsourceTest] {
//  定义一个FLAG，表示数据源是否正常运行
  var running:Boolean=true
  override def run(ctx: SourceFunction.SourceContext[SendsoRendsourceTest]): Unit = {
    val rand = new Random()
    var curTemps = 1.to(10).map(
      i=>("sensor"+i,60 +rand.nextGaussian()*20)
    )
    //无限循环，生成循环数据
    while (running){
      //随机生成微小波动
      curTemps.map(
        data=> (data._1,data._2+rand.nextGaussian())
      )
      val cutrs=System.currentTimeMillis()
      //包装成样例类，用ctx发送数据
      curTemps.foreach(
        data=>ctx.collect(SendsoRendsourceTest(data._1,cutrs,data._2))
      )
     Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    running=false
  }
}

