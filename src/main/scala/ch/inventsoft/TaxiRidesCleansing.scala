package ch.inventsoft

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object TaxiRidesCleansing {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val maxDelay = 0
    val servingSpeed = 600
    val rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed))

    val out = rides
      .filter(ride => GeoUtils.isInNYC(ride.endLon, ride.endLat) && GeoUtils.isInNYC(ride.startLon, ride.startLat))

    out.print()
    env.execute("Taxi Rides Cleansing")
  }
}
