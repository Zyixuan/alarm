package Flink_Test_excutionEnvironment

import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val init_steam: DataStream[String] = env.readTextFile("E:\\java_home_work\\IdeaProjects\\zbs_learn\\zbs_spring\\src\\main\\resources\\Tess_data")
   init_steam.flatMap(_.split(" ")).map((_,1)).keyBy(1).sum(1).print()
  env.execute("ceshishuju")

  }

}
