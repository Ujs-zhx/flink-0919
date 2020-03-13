package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Program: flink-0919
  * ClassName WorldCount
  * Description: ${description}
  * Author: Mr.Zhang
  * Version 1.0
  * Create: 2020-03-13 20:15
  **/
object WorldCount {
  def main(args: Array[String]): Unit = {
    //1.创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  //2.从文件中读取数据
    val inputPath = "E:\\workspace_idea191130\\flink-0919\\input\\Hello.txt"
  val inputDateSet: DataSet[String] = env.readTextFile(inputPath)
    val unit: AggregateDataSet[(String, Int)] = inputDateSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
//    val resultDataSet: DataSet[(String, Int)] = inputDataSet
//      .flatMap( _.split(" ") )     // 先把每一行数据按照空格分词
//      .map( (_, 1) )    // 包装成(word, count)二元组
//      .groupBy(0)    // 基于二元组中的第一个元素，也就是word做分组
//      .sum(1)    // 将每组中按照二元组的第二个元素，也就是count做聚合
//
//    // 4. 打印输出

    unit.print()
  }
}
