package ch.inventsoft

import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09
import org.apache.flink.streaming.util.serialization.{SerializationSchema, TypeInformationSerializationSchema}
import org.apache.flink.util.Collector

object TaxiRidesPopular {
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.getRequired("input")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val maxDelay = 0
    val servingSpeed = 600
    val rides = env.addSource(new TaxiRideSource(input, maxDelay, servingSpeed))

    val minToBeRelevant = 5

    val out = rides
      .keyBy { ride =>
        if (ride.isStart) RideCell(true, GeoUtils.mapToGridCell(ride.startLon, ride.startLat))
        else RideCell(false, GeoUtils.mapToGridCell(ride.endLon, ride.endLat))
      }
      .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
      .apply { (key, window, values, out: Collector[PopularLocation]) =>
        val result = PopularLocation(key.lon, key.lat, window.getEnd, key.isStart, values.size)
        out.collect(result)
      }
      .filter(_.count >= minToBeRelevant)

    val kafka = new FlinkKafkaProducer09[PopularLocation]("localhost:9092", "taxi-rides-popular", PopularLocation.schema(env.getConfig))
    out.addSink(kafka)

    env.execute("Taxi Rides Popular")
  }

  case class RideCell(isStart: Boolean, cell: Int) {
    def lon = GeoUtils.getGridCellCenterLon(cell)
    def lat = GeoUtils.getGridCellCenterLat(cell)
  }
  case class PopularLocation(lon: Float, lat: Float, time: Long, isStart: Boolean, count: Int)
  object PopularLocation {
    def schema(implicit config: ExecutionConfig): SerializationSchema[PopularLocation] = {
      val info = TypeInformation.of(new TypeHint[PopularLocation](){})
      new TypeInformationSerializationSchema[PopularLocation](info, config)
    }
  }
}
