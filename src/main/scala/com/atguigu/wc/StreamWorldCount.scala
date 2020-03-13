package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Program: flink-0919
  * ClassName StreamWorldCount
  * Description: ${description}
  * Author: Mr.Zhang
  * Version 1.0
  * Create: 2020-03-13 20:26
  **/
object StreamWorldCount {
  def main(args: Array[String]): Unit = {
    //1.创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  //2.从socket文本流中读取数据
  val params: ParameterTool = ParameterTool.fromArgs(args)
  val host: String = params.get("host")
  val port: Int = params.getInt("port")
  val inputStream: DataStream[String] = env.socketTextStream(host,port)
  //3.对数据进行处理
  // 指定以二元组中第一个元素，也就是word作为key，然后按照key分组
    val result: DataStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
  result.print()
    env.execute()
  }
}
