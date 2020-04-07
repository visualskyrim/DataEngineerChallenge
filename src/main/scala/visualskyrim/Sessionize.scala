package visualskyrim

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import scopt.OptionParser
import visualskyrim.common.{AppConf, DateTimeUtils}
import visualskyrim.processes.{InputResolver, Sessionizer}
import visualskyrim.schema.{Normalized, SessionCutWatermark}


case class SessionizeOptions(hour: String = null)

object SessionizeOptions {

  def parser: OptionParser[SessionizeOptions] = new OptionParser[SessionizeOptions]("Sessionize") {
    head("Sessionize")

    opt[String]("hour")
      .required()
      .action((arg, option) => option.copy(hour = arg))
      .text("hour in yyyy-MM-ddTHH.")

  }
}

object Sessionize extends App {
  SessionizeOptions.parser.parse(args, SessionizeOptions()) match {
    case None => throw new RuntimeException("Fail to parse the parameters.")
    case Some(SessionizeOptions(hour)) =>

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

      val normalizedDS = normalizedEitherDS
        .filter(x => x.isLeft)
        .map(x => x.left.get)
        .toDS()

      // Get pending accesses from the previous hour
      val watermarkInputPath = DateTimeUtils.getHourlyBatchPartition(appConf.pending, batchHour.minusHours(1))

      val mergedInput: Dataset[SessionCutWatermark] = if (fs.exists(new org.apache.hadoop.fs.Path(watermarkInputPath))) {
        spark.read.parquet(watermarkInputPath).as[SessionCutWatermark]
      } else {
        spark.emptyDataset[SessionCutWatermark]
      }

      val sessionizedDS = normalizedDS
        .groupByKey(x => x.clientId)
        .cogroup(
          mergedInput.groupByKey(x => x.clientId)) { (clientId, accessIter, watermarkIter) =>

          val watermark = if (watermarkIter.isEmpty) {
            SessionCutWatermark()
          }
          else {
            assert(watermarkIter.toSeq.size == 1)
            watermarkIter.toSeq.head
          }

          Seq(Sessionizer.sessionize(accessIter.toSeq, watermark, batchHour))
        }


      sessionizedDS.cache()

      sessionizedDS.flatMap(x => x.sessions).write.parquet(DateTimeUtils.getHourlyBatchPartition(appConf.sessionized, batchHour))
      sessionizedDS.map(x => x.watermark).write.parquet(DateTimeUtils.getHourlyBatchPartition(appConf.pending, batchHour))
  }

}
