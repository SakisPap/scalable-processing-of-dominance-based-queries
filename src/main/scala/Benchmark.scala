import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.util.LongAccumulator
import org.joda.time.{DateTime, Duration}

/**
 * this Object contains the benchmark execution as described in the assignment
 * task 1: Computes d-dimensional points that are NOT dominated (skyline)
 * task 2: Computes k points that have the highest dominance score
 * task 3: Compotes k points from skyline that have the highest dominance score
 * to ensure scalability and performance, the code employs RDDs, accumulators, Spark SQL
 * and dataframes. Additionally, Sort-Filter-Skyline" (SFS) is used to increase
 * computational efficiency
 */
object Benchmark {
  /**
   * run benchmark tasks.
   * @param spark The SparkSession.
   * @param csvFilepath The path to the CSV file.
   * @param k The value of K for top-k tasks.
   */
  def run(spark: SparkSession, csvFilepath: String, k: Int): Unit = {
    // read CSV file into DataFrame
    val df = spark.read.option("inferSchema", "true").csv(csvFilepath)

    // record starting time
    val startTime = DateTime.now()
    println("stopwatch has been started!")

    // perform skyline query task
    val skylineSet = skylineQuery(spark, df)
    println(s"(task 1) Skyline set found: ${skylineSet.mkString(", ")}")

    // record time taken for skyline query task
    val skylineTime = DateTime.now()
    val skylineDuration = new Duration(startTime, skylineTime)
    val skylineTimeSecs = skylineDuration.getMillis / 1000.0

    // perform top-k dominating task
    topKDominating(k, spark, df)
    val topKDominatingTime = DateTime.now()
    val topKDominatingDuration = new Duration(skylineTime, topKDominatingTime)
    val topKDominatingTimeSecs = topKDominatingDuration.getMillis / 1000.0

    // perform top-k skyline task
    topKSkyline(k, spark, df)
    val topKSkylineTime = DateTime.now()
    val topKSkylineDuration = new Duration(topKDominatingTime, topKSkylineTime)
    val topKSkylineTimeSecs = topKSkylineDuration.getMillis / 1000.0

    // calculate total execution time
    val totalTime = skylineDuration.getMillis + topKDominatingDuration.getMillis + topKSkylineDuration.getMillis
    println("stopwatch has been stopped!")

    println(s"1. skyline computation time: $skylineTimeSecs sec")
    println(s"2. top-k dominance computation time: $topKDominatingTimeSecs sec")
    println(s"3. top-k skyline dominance computation time: $topKSkylineTimeSecs sec")
    println(s"TOTAL (1+2+3) time: ${totalTime / 1000.0} sec")
  }

  /**
   * Perform skyline query task.
   * @param spark The SparkSession.
   * @param df The DataFrame.
   * @return ArrayBuffer[Row] containing skyline points.
   */
  private def skylineQuery(spark: SparkSession, df: DataFrame): ArrayBuffer[Row] = {
    // initialize collection accumulator to store skyline points
    val skylineAccumulator = spark.sparkContext.collectionAccumulator[Row]("skylineAccumulator")

    // compute sum of each row and sort by sum
    val sumDF = df.withColumn("sum", df.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))
    val sortedSumDF = sumDF.sort(col("sum").asc)

    // compute local skyline and return global skyline
    computeLocalSkyline(sortedSumDF, skylineAccumulator)
    computeGlobalSkyline(skylineAccumulator)
  }

  /**
   * Perform top-k dominating task.
   * @param k The value of K for top-k.
   * @param spark The SparkSession.
   * @param df The DataFrame.
   */
  private def topKDominating(k: Int, spark: SparkSession, df: DataFrame): Unit = {
    var changingDf = df
    for (i <- 1 to k) {
      val skylinePoints: ArrayBuffer[Row] = skylineQuery(spark, changingDf)
      val scoreAcc = spark.sparkContext.longAccumulator("accumulator")
      var dominatingMap = Map[Row, Long]()

      // calculate dominance score for each skyline point
      for (row <- skylinePoints) {
        scoreAcc.reset()
        dominantScore(row, changingDf, scoreAcc)
        dominatingMap = dominatingMap + (row -> scoreAcc.value)
      }

      // sort points by dominance score and print top-k
      val sortedDominatingMap = dominatingMap.toSeq.sortWith(_._2 > _._2)
      val (topPoint, topValue) = sortedDominatingMap.head
      println(s"(task 2) top-$i : * $topPoint * SCORE: $topValue")

      // remove top point from dataset
      changingDf = changingDf.filter(r => {
        val dimensions = r.length - 1
        val pointDimensions = Array.fill(dimensions){0.0}
        val topPointDimensions = Array.fill(dimensions){0.0}

        for (i <- 0 until dimensions) {
          pointDimensions(i) += r.getDouble(i)
          topPointDimensions(i) += topPoint.getDouble(i)
        }

        !pointDimensions.sameElements(topPointDimensions)
      })
    }
  }

  /**
   * Perform top-k skyline task.
   * @param k The value of K for top-k.
   * @param spark The SparkSession.
   * @param df The DataFrame.
   */
  private def topKSkyline(k: Int, spark: SparkSession, df: DataFrame): Unit = {
    val skylinePoints = skylineQuery(spark, df)
    val scoreAcc = spark.sparkContext.longAccumulator("accumulator")
    var dominatingMap = Map[Row, Long]()

    // calculate dominance score for each skyline point
    for (row <- skylinePoints) {
      scoreAcc.reset()
      dominantScore(row, df, scoreAcc)
      dominatingMap = dominatingMap + (row -> scoreAcc.value)
    }

    // sort points by dominance score and print top-k
    val sortedDominatingMap = dominatingMap.toSeq.sortWith(_._2 > _._2)
    val numOfElements = sortedDominatingMap.size

    var i = 1
    for ((topK, topV) <- sortedDominatingMap) {
      println(s"(task 3) top-$i in skyline is point * $topK * SCORE: $topV")
      i = i + 1
      if (i > k){
        return
      }
    }
    if (k > numOfElements) {
      println(s"Skyline empty!")
    }
  }

  /**
   * Compute global skyline from accumulated local skyline points.
   * @param skylineAccumulator The CollectionAccumulator containing local skyline points.
   * @return ArrayBuffer[Row] containing global skyline points.
   */
  private def computeGlobalSkyline(skylineAccumulator: CollectionAccumulator[Row]): ArrayBuffer[Row] = {
    var sortedPoints = new ArrayBuffer[Row]()
    skylineAccumulator.value.forEach( r => {
      sortedPoints += r
    })
    sortedPoints = sortedPoints.sortBy(r => r.getDouble(r.length - 1))

    var globalSkyline = new ArrayBuffer[Row]()

    sortedPoints.foreach( r => {
      val dimensions = r.length - 1

      // array that represents the dimensions (coordinates) of the examined point
      val pointDimensions = Array.fill(dimensions) {
        0.0
      }
      for (i <- 0 until dimensions) {
        pointDimensions(i) += r.getDouble(i)
      }

      var isDominated = false

      for (x <- globalSkyline) {

        // array that represents the dimensions (coordinates) of the skyline point
        val skylineDimensions = Array.fill(dimensions) {
          0.0
        }
        for (i <- 0 until dimensions) {
          skylineDimensions(i) += x.getDouble(i)
        }

        if (isPointDominated(dimensions, pointDimensions, skylineDimensions)) {
          isDominated = true
        }
      }

      if (!isDominated) {
        globalSkyline += r
      }
    })
    globalSkyline
  }

  /**
   * Compute local skyline points.
   * @param df The DataFrame partition.
   * @param skylineAccumulator The CollectionAccumulator to store local skyline points.
   */
  private def computeLocalSkyline(df: DataFrame, skylineAccumulator: CollectionAccumulator[Row]): Unit = {
    val rowsRDD: RDD[Row] = df.rdd

    rowsRDD.foreachPartition( iterator => {

      var localSkyline = new ArrayBuffer[Row]()

      iterator.foreach(row => {
        val dimensions = row.length - 1

        // array that represents the dimensions (coordinates) of the examined point
        val pointDimensions = Array.fill(dimensions) {
          0.0
        }
        for (i <- 0 until dimensions) {
          pointDimensions(i) += row.getDouble(i)
        }

        var isDominated = false

        for (x <- localSkyline) {

          // array that represents the dimensions (coordinates) of the skyline point
          val skylineDimensions = Array.fill(dimensions) {
            0.0
          }
          for (i <- 0 until dimensions) {
            skylineDimensions(i) += x.getDouble(i)
          }

          if (isPointDominated(dimensions, pointDimensions, skylineDimensions)) {
            isDominated = true
          }
        }

        if (!isDominated) {
          localSkyline += row
        }
      })
      localSkyline.foreach( r => {
        skylineAccumulator.add(r)
      })
    })
  }

  /**
   * Check if a point is dominated by another point.
   * @param dimensions The number of dimensions.
   * @param point The dimensions of the point.
   * @param skylinePoint The dimensions of the skyline point.
   * @return true if the point is dominated, false otherwise.
   */
  private def isPointDominated(dimensions: Int, point: Array[Double], skylinePoint: Array[Double]): Boolean = {
    val atLeastAsGood = Array.fill(dimensions){false}

    for ( i <- 0 until dimensions) {
      if (skylinePoint(i) < point(i)) {
        atLeastAsGood(i) = true
      }
    }

    val res = atLeastAsGood.reduce((a, b) => a && b)
    res
  }

  /**
   * Calculate the dominance score of a point.
   * @param row The Row representing the point.
   * @param dataset The DataFrame containing the dataset.
   * @param scoreAcc The LongAccumulator to store the score.
   */
  private def dominantScore(row: Row,
                            dataset: DataFrame,
                            scoreAcc: LongAccumulator): Unit = {
    val dimensions = row.length - 1

    // array that represents the dimensions (coordinates) of the examined point
    val pointDimensions = Array.fill(dimensions){0.0}
    for ( i <- 0 until dimensions) {
      pointDimensions(i) += row.getDouble(i)
    }

    dataset.foreach(r => {

      val otherPointDimensions = Array.fill(dimensions){0.0}
      for ( i <- 0 until dimensions) {
        otherPointDimensions(i) += r.getDouble(i)
      }

      if (isPointDominated(dimensions, otherPointDimensions, pointDimensions)) {
        scoreAcc.add(1)
      }
    })
  }
}
