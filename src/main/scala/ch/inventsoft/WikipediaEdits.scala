package ch.inventsoft

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

object WikipediaEdits {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val edits = env.addSource(new WikipediaEditsSource())

    val byteDiffs = edits
      .map(_.getByteDiff)
      .timeWindowAll(Time.seconds(5))
      .fold((0, 0L))((s, e) => {
        val (count, totalDiff) = s
        (count + 1, totalDiff + e)
      })
      .map(r => r._2 / r._1)

    byteDiffs.print()


    env.execute("Wikipedia Edits example")
  }
}