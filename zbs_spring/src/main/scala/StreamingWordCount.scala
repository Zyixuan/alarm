import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.guava18.com.google.common.reflect.Parameter
import org.apache.flink.streaming.api.scala._

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接受socket文本流 以本地接口7777接受文本流数据
    var param: ParameterTool = ParameterTool.fromArgs(args)
    var param2: ParameterTool = ParameterTool.fromPropertiesFile("/Http.properties")
    var host2: String = param2.get("Host")

    var host: String = param.get("Host")
    var tool: Int = param.getInt("port")
    var inputStream: DataStream[String] = env.socketTextStream("loaclhost", 7777)


    var outputSteam: DataStream[(String, Int)] = inputStream
      .flatMap(_.split(" ")) // 数据分割打平
      .filter(_.nonEmpty)
      .map(new MymapFunction)  // 拼接成二元组
      .keyBy(0) // 以第一个字段分组
      .sum(1)  //以第二字段聚合 求出count 值
    val Spiltvalue: SplitStream[(String, Int)] = outputSteam.split(date => {
      if (date._2 > 2)
        Seq("high")
      else
        Seq("low")
    })
    val resultvalue: DataStream[(String, Int)] = Spiltvalue.select("high")
    val connectStream: ConnectedStreams[(String, Int), (String, Int)] = resultvalue.connect(outputSteam)
    connectStream.flatMap(data1=>{data1._1.split("")},data2=>{data2._1.split("")})
    outputSteam.print()   //打印显示值
    env.execute("Flink Stream test")

  }

}

//自定义mapFunction
class MymapFunction extends RichMapFunction[String,(String,Int)]{
  override def map(t: String): (String, Int) = {
    (t,1)
  }
}
