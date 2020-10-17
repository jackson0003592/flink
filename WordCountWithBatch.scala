package com.atguigu.day1

import org.apache.flink.streaming.api.scala._

object WordCountWithBatch {

  case class WordWithCount(str: String, i: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "hello world",
      "hello world"
    )

    val result = stream.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy(0)
      .sum(1)

    result.print()

    env.execute("batch word count")
  }

}
