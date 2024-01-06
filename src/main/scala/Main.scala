import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {
    println("Running Main!")

    // Suppressing log4j logger because it trashes the console output
    Logger.getRootLogger.setLevel(Level.WARN)

    val INPUT_DATASET_PATH_2D_ANTI = "datasets/dimensions_3-dist_correlated-points_1000000.csv"
    val K_VALUE = 5

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("scalable-processing-of-dominance-based-queries")
      .config("spark.executor.memory", "8g")
      .config("spark.executor.cores", "4")
      .getOrCreate()

    val benchmark = Benchmark

    val dimensions = Seq(2, 3, 4, 5)
    val distributions = Seq("correlated", "anticorrelated", "normal", "uniform")

    for {
      dimension <- dimensions
      distribution <- distributions
    } yield {
      println(s"\n==== Benchmark has started | DIMENSION: ${dimension}D | DISTRIBUTION: ${distribution} | K: ${K_VALUE} ====\n")
      benchmark.run(spark, s"datasets/one_hundred_thousand_points/dimensions_${dimension}-dist_${distribution}-points_100000.csv", K_VALUE)
    }
  }
}