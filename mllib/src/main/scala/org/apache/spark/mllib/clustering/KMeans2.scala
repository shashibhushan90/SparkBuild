//KMeans2
package org.apache.spark.mllib.clustering

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

import org.apache.spark.{SparkContext, Logging}

import scala.util.control.Breaks

//import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom


class KMeans2 private (
    private var k: Int,
    private var maxIterations: Int,
    private var runs: Int,
    private var initializationMode: String,
    private var initializationSteps: Int,
    private var epsilon: Double,
    private var seed: Long) extends KMeans with ModelEvaluation {
  /**
   * Constructs a KMeans instance with default parameters: {k: 2, maxIterations: 20, runs: 1,
   * initializationMode: "k-means||", initializationSteps: 5, epsilon: 1e-4, seed: random}.
   */
  def this() = this(2, 20, 1, KMeans2.K_MEANS_PARALLEL, 5, 1e-4, Utils.random.nextLong())
/**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */

  override def run(data: RDD[Vector]): KMeansModel = {

    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }

    // Compute squared norms and cache them.
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map { case (v, norm) =>
      new VectorWithNorm(v, norm)
    }
    val model = runAlgorithm(zippedData, data)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

 /*def evaluate(
       bestRun: Int,
       centers: Array[Array[VectorWithNorm]],
       data1: RDD[Vector]):Double =
       {
             val dynamicModel = new KMeansModel(centers(bestRun).map(_.vector))
             val WSSSE = dynamicModel.computeCost(data1)
 	           return WSSSE
      } */



  private def runAlgorithm(data: RDD[VectorWithNorm], data1: RDD[Vector], mypath: String = "/user/hduser/"): KMeansModel = {

    val sc = data.sparkContext
    val loop = new Breaks

    val initStartTime = System.nanoTime()
    val toggle: Boolean = false
    val centers = if (initializationMode == KMeans2.RANDOM) {
      initRandom(data)
    } else {
      initKMeansParallel(data)
    }

    val initTimeInSeconds = (System.nanoTime() - initStartTime) / 1e9
    logInfo(s"Initialization with $initializationMode took " + "%.3f".format(initTimeInSeconds) +
      " seconds.")

    val active = Array.fill(runs)(true)
    val costs = Array.fill(runs)(0.0)

    var activeRuns = new ArrayBuffer[Int] ++ (0 until runs)
    var iteration = 0

    val iterationStartTime = System.nanoTime()
    loop.breakable {
      // Execute iterations of Lloyd's algorithm until all runs have converged
      while (iteration < maxIterations && !activeRuns.isEmpty) {
        type WeightedPoint = (Vector, Long)
        def mergeContribs(x: WeightedPoint, y: WeightedPoint): WeightedPoint = {
          axpy(1.0, x._1, y._1)
          (y._1, x._2 + y._2)
        }

        val activeCenters = activeRuns.map(r => centers(r)).toArray
        val costAccums = activeRuns.map(_ => sc.accumulator(0.0))

        val bcActiveCenters = sc.broadcast(activeCenters)

        // Find the sum and count of points mapping to each center
        val totalContribs = data.mapPartitions { points =>
          val thisActiveCenters = bcActiveCenters.value
          val runs = thisActiveCenters.length
          val k = thisActiveCenters(0).length
          val dims = thisActiveCenters(0)(0).vector.size

          val sums = Array.fill(runs, k)(Vectors.zeros(dims))
          val counts = Array.fill(runs, k)(0L)

          points.foreach { point =>
            (0 until runs).foreach { i =>
              val (bestCenter, cost) = KMeans2.findClosest(thisActiveCenters(i), point)
              costAccums(i) += cost
              val sum = sums(i)(bestCenter)
              axpy(1.0, point.vector, sum)
              counts(i)(bestCenter) += 1
            }
          }

          val contribs = for (i <- 0 until runs; j <- 0 until k) yield {
            ((i, j), (sums(i)(j), counts(i)(j)))
          }
          contribs.iterator
        }.reduceByKey(mergeContribs).collectAsMap()





        // Update the cluster centers and costs for each active run
        for ((run, i) <- activeRuns.zipWithIndex) {
          var changed = false
          var j = 0
          while (j < k) {
            val (sum, count) = totalContribs((i, j))
            if (count != 0) {
              scal(1.0 / count, sum)
              val newCenter = new VectorWithNorm(sum)
              if (KMeans2.fastSquaredDistance(newCenter, centers(run)(j)) > epsilon * epsilon) {
                changed = true
              }
              centers(run)(j) = newCenter
            }
            j += 1
          }
          if (!changed) {
            active(run) = false
            logInfo("Run " + run + " finished in " + (iteration + 1) + " iterations")
          }
          costs(run) = costAccums(i).value
        }

        activeRuns = activeRuns.filter(active(_))

        iteration += 1
        //trait evaluate
        if ((iteration < maxIterations)) {
          val (minCost, bestRun) = costs.zipWithIndex.min
          val WSE = evaluate(bestRun, centers, data1)
          val toggle: Boolean = true
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"------------------------------start  ----------------------------------------------------------------")
          logInfo(s"Within Set Sum of Squared Errors = $WSE at iteration: $iteration")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          //trait compare
          val tempmodel = new KMeansModel(centers(bestRun).map(_.vector))
          val savePath: String = mypath  + sc.appName + "Iteration" + iteration
          logInfo(s"Model at iteration number: $iteration is saved in $savePath")
          tempmodel.save(sc, savePath)
          loop.break()
        }

      }
    }
    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9

    if (toggle == true) {
      logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")
      val loadPath: String = mypath + sc.appName + "Iteration" + iteration

      if (iteration == maxIterations) {
        logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
        val loadPath: String = mypath + sc.appName + "Iteration" + maxIterations
      }
      else {
        logInfo(s"KMeans converged in $iteration iterations.")
      }

      KMeansModel.load(sc, loadPath)
    }
    else {
      logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")

      if (iteration == maxIterations) {
        logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
      } else {
        logInfo(s"KMeans converged in $iteration iterations.")
      }

      val (minCost, bestRun) = costs.zipWithIndex.min

      logInfo(s"The cost for the best run is $minCost.")
      new KMeansModel(centers(bestRun).map(_.vector))
    }
  }

    /**
   * Initialize `runs` sets of cluster centers at random.
   */
  private def initRandom(data: RDD[VectorWithNorm])
  : Array[Array[VectorWithNorm]] = {
    // Sample all the cluster centers in one pass to avoid repeated scans
    val sample = data.takeSample(true, runs * k, new XORShiftRandom(this.seed).nextInt()).toSeq
    Array.tabulate(runs)(r => sample.slice(r * k, (r + 1) * k).map { v =>
      new VectorWithNorm(Vectors.dense(v.vector.toArray), v.norm)
    }.toArray)
  }

  /**
   * Initialize `runs` sets of cluster centers using the k-means|| algorithm by Bahmani et al.
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). This is a variant of k-means++ that tries
   * to find with dissimilar cluster centers by starting with a random center and then doing
   * passes where more centers are chosen with probability proportional to their squared distance
   * to the current cluster set. It results in a provable approximation to an optimal clustering.
   *
   * The original paper can be found at http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf.
   */
  private def initKMeansParallel(data: RDD[VectorWithNorm])
  : Array[Array[VectorWithNorm]] = {
    // Initialize empty centers and point costs.
    val centers = Array.tabulate(runs)(r => ArrayBuffer.empty[VectorWithNorm])
    var costs = data.map(_ => Vectors.dense(Array.fill(runs)(Double.PositiveInfinity))).cache()

    // Initialize each run's first center to a random point.
    val seed = new XORShiftRandom(this.seed).nextInt()
    val sample = data.takeSample(true, runs, seed).toSeq
    val newCenters = Array.tabulate(runs)(r => ArrayBuffer(sample(r).toDense))

    /** Merges new centers to centers. */
    def mergeNewCenters(): Unit = {
      var r = 0
      while (r < runs) {
        centers(r) ++= newCenters(r)
        newCenters(r).clear()
        r += 1
      }
    }

    // On each step, sample 2 * k points on average for each run with probability proportional
    // to their squared distance from that run's centers. Note that only distances between points
    // and new centers are computed in each iteration.
    var step = 0
    while (step < initializationSteps) {
      val bcNewCenters = data.context.broadcast(newCenters)
      val preCosts = costs
      costs = data.zip(preCosts).map { case (point, cost) =>
        Vectors.dense(
          Array.tabulate(runs) { r =>
            math.min(KMeans2.pointCost(bcNewCenters.value(r), point), cost(r))
          })
      }.cache()
      val sumCosts = costs
        .aggregate(Vectors.zeros(runs))(
          seqOp = (s, v) => {
            // s += v
            axpy(1.0, v, s)
            s
          },
          combOp = (s0, s1) => {
            // s0 += s1
            axpy(1.0, s1, s0)
            s0
          }
        )
      preCosts.unpersist(blocking = false)
      val chosen = data.zip(costs).mapPartitionsWithIndex { (index, pointsWithCosts) =>
        val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
        pointsWithCosts.flatMap { case (p, c) =>
          val rs = (0 until runs).filter { r =>
            rand.nextDouble() < 2.0 * c(r) * k / sumCosts(r)
          }
          if (rs.length > 0) Some(p, rs) else None
        }
      }.collect()
      mergeNewCenters()
      chosen.foreach { case (p, rs) =>
        rs.foreach(newCenters(_) += p.toDense)
      }
      step += 1
    }

    mergeNewCenters()
    costs.unpersist(blocking = false)

    // Finally, we might have a set of more than k candidate centers for each run; weigh each
    // candidate by the number of points in the dataset mapping to it and run a local k-means++
    // on the weighted centers to pick just k of them
    val bcCenters = data.context.broadcast(centers)
    val weightMap = data.flatMap { p =>
      Iterator.tabulate(runs) { r =>
        ((r, KMeans2.findClosest(bcCenters.value(r), p)._1), 1.0)
      }
    }.reduceByKey(_ + _).collectAsMap()
    val finalCenters = (0 until runs).par.map { r =>
      val myCenters = centers(r).toArray
      val myWeights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i), 0.0)).toArray
      LocalKMeans.kMeansPlusPlus(r, myCenters, myWeights, k, 30)
    }

    finalCenters.toArray
  }

}

/**
 * Top-level methods for calling K-means clustering.
 */
object KMeans2 {

  // Initialization mode names
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @param k number of clusters
   * @param maxIterations max number of iterations
   * @param runs number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   * @param seed random seed value for cluster initialization
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String,
      seed: Long): KMeansModel = {
    new KMeans2().setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .run(data)
  }

  /**
   * Trains a k-means model using the given set of parameters.
   *
   * @param data training points stored as `RDD[Vector]`
   * @param k number of clusters
   * @param maxIterations max number of iterations
   * @param runs number of parallel runs, defaults to 1. The best model is returned.
   * @param initializationMode initialization model, either "random" or "k-means||" (default).
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int,
      initializationMode: String): KMeansModel = {
    new KMeans2().setK(k)
      .setMaxIterations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .run(data)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int): KMeansModel = {
    train(data, k, maxIterations, 1, K_MEANS_PARALLEL)
  }

  /**
   * Trains a k-means model using specified parameters and the default values for unspecified.
   */
  def train(
      data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      runs: Int): KMeansModel = {
    train(data, k, maxIterations, runs, K_MEANS_PARALLEL)
  }

  /**
   * Returns the index of the closest center to the given point, as well as the squared distance.
   */
  private[mllib] def findClosest(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      // Since `\|a - b\| \geq |\|a\| - \|b\||`, we can use this lower bound to avoid unnecessary
      // distance computation.
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
   * Returns the K-means cost of a given point against the given cluster centers.
   */
  private[mllib] def pointCost(
      centers: TraversableOnce[VectorWithNorm],
      point: VectorWithNorm): Double =
    findClosest(centers, point)._2

  /**
   * Returns the squared Euclidean distance between two vectors computed by
   * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
   */
  private[clustering] def fastSquaredDistance(
      v1: VectorWithNorm,
      v2: VectorWithNorm): Double = {
    MLUtils.fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }
}