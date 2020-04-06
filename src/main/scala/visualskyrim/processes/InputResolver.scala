package visualskyrim.processes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

object InputResolver {
  // TODO: When the input data is partitioned in hourly data, change the `resolve` to read from hourly partition
  def resolve(basePath: String, datehour: DateTime)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .option("delimiter", " ")
      .csv(s"/user/ratuser/work/chris/payground/%d_%02d_%02d_mktplace_shop_web_log_sample.log.gz"
        .format(datehour.getYear, datehour.getMonthOfYear, datehour.getDayOfMonth))
  }


}
