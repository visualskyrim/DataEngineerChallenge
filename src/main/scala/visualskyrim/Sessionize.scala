package visualskyrim

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import scopt.OptionParser
import visualskyrim.common.{AppConf, DateTimeUtils}
import visualskyrim.processes.{InputResolver, Sessionizer}
import visualskyrim.schema.{Normalized, SessionCutWatermark}


case class SessionizeOptions(hour: String = null, firstHour: Boolean = false)

object SessionizeOptions {

  def parser: OptionParser[SessionizeOptions] = new OptionParser[SessionizeOptions]("Sessionize") {
    head("Sessionize")

    opt[String]("hour")
      .required()
      .action((arg, option) => option.copy(hour = arg))
      .text("hour in yyyy-MM-ddTHH.")

    opt[Unit]("firstHour").action((arg, option) =>
      option.copy(firstHour = true)).text("Remove existing output before (re)running.")

  }
}

object Sessionize extends App {
  SessionizeOptions.parser.parse(args, SessionizeOptions()) match {
    case None => throw new RuntimeException("Fail to parse the parameters.")
    case Some(SessionizeOptions(hour, firstHour)) =>

      implicit val spark: SparkSession = SparkSession.builder()
        .appName(s"Chris Check | $hour")
        .enableHiveSupport()
        .getOrCreate()

      import spark.implicits._
      val normalizedSchema = ScalaReflection.schemaFor[Normalized].dataType.asInstanceOf[StructType]

      val sc = spark.sparkContext
      val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

      val batchHour = DateTimeUtils.fromBatchHour(hour)

      val appConf = AppConf()
      val inputDS = InputResolver.resolve(appConf.input, batchHour)


      // TODO: re-Partitioning if needed
      val normalizedEitherDS = inputDS.rdd
        .map(row => Normalized(row)).cache() // TODO: choose cache level depending on the infra

      normalizedEitherDS.count() // Trigger caching

      normalizedEitherDS.filter(x => x.isRight).map(x => x.right.get).toDS().write.text(DateTimeUtils.getHourlyBatchPartition(appConf.error, batchHour))

      val normalizedRDD = normalizedEitherDS
        .filter(x => x.isLeft)
        .map(x => x.left.get)


      // Get pending accesses from the previous hour
      val watermarkInputPath = DateTimeUtils.getHourlyBatchPartition(appConf.pending, batchHour.minusHours(1))

      val pendingRDD = (if (firstHour) {
        spark.emptyDataset[SessionCutWatermark]
      } else {
        spark.read.parquet(watermarkInputPath).as[SessionCutWatermark]
      }).rdd

      val sessionizedDS = normalizedRDD
        .groupBy(x => x.clientId)
        .fullOuterJoin(pendingRDD.groupBy(x => x.clientId)) //TODO: Add partitioner if shuffling is worthy
        .map { case (clientId, (accessesOpt, watermarksOpt)) =>

        val watermark = watermarksOpt match {
          case Some(watermarks) =>
            assert(watermarks.size == 1)
            watermarks.head
          case None => SessionCutWatermark()
        }

        Sessionizer.sessionize(accessesOpt.getOrElse(Iterable.empty).toSeq, watermark, batchHour)
      }

      sessionizedDS.cache()

      sessionizedDS.flatMap(x => x.sessions).toDS().write.parquet(DateTimeUtils.getHourlyBatchPartition(appConf.sessionized, batchHour))
      sessionizedDS.map(x => x.watermark).filter(x => !x.clientId.isEmpty).toDS().write.parquet(DateTimeUtils.getHourlyBatchPartition(appConf.pending, batchHour))

  }
}
