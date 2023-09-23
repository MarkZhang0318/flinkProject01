package org.bw.flink1

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment,_}

object Job5 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("hadoop5", 9999)
    val result = source.flatMap(line => line.split(",")).map((_, 1)).keyBy(0).sum(1)
    result.print()
    env.execute("job5")
  }

}
