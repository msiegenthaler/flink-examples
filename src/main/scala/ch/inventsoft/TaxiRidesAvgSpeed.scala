package ch.inventsoft

import java.util.concurrent.TimeUnit
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.joda.time.Duration

object TaxiRidesAvgSpeed {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val maxDelay = 0
    val servingSpeed = 60
    val rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed))

    val out = rides
      .filter(ride => GeoUtils.isInNYC(ride.endLon, ride.endLat) && GeoUtils.isInNYC(ride.startLon, ride.startLat))
      .keyBy(_.rideId)
      .fold(RideStartEnd()) { (s, ride) =>
        if (ride.isStart) s.copy(start = Some(ride))
        else s.copy(end = Some(ride))
      }
      .flatMap(ride => ride.speed.map(s => (ride.start.get.rideId, s)))

    out.print()
    env.execute("Taxi Rides AvgSpeed")
  }

  case class RideStartEnd(start: Option[TaxiRide] = None, end: Option[TaxiRide] = None) {
    def duration = {
      for {
        s <- start
        e <- end
      } yield new Duration(s.time, e.time)
    }
    def speed = {
      for {
        e <- end
        t <- duration
        x = e.travelDistance
      } yield x / t.getMillis * TimeUnit.HOURS.toMillis(1)
    }
    def isComplete = start.isDefined && end.isDefined
  }
}
