//KMeans3
package org.apache.spark.mllib.clustering

import breeze.optimize.MaxIterations

import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{ApplicationEventListener, SparkListenerRMRequest, LiveListenerBus}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
 * K-means clustering with support for multiple parallel runs and a k-means++ like initialization
 * mode (the k-means|| algorithm by Bahmani et al). When multiple concurrent runs are requested,
 * they are executed together with joint passes over the data for efficiency.
 *
 * This is an iterative algorithm that will make multiple passes over the data, so any RDDs given
 * to it should be cached by the user. Here KMeans is extended and changes to the RunAlgorithm is made to
 */

class KMeans3 (
                var k: Int,
                var maxIterations: Int,
                var runs:Int,
                var initializationMode: String
                ) extends KMeans with ModelEvaluation {


  var epsilon = new KMeans().getEpsilon
  var initializationSteps = new KMeans().getInitializationSteps
  var seed = new KMeans().getSeed


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
    val model = runAlgorithm2(zippedData, data)
    norms.unpersist()

    // Warn at the end of the run as well, for increased visibility.
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    model
  }

  val mylist = List(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)

  def is10percent(iteration: Int): Boolean = {
    //val maxIterations = 30
    val iterationInterval = (maxIterations/10).toDouble
    val temp = (iteration/(iterationInterval))

    if(mylist.contains(temp)) {
      return true
    }
    else {
      return false
    }
  }





  private def runAlgorithm2(data: RDD[VectorWithNorm], data1: RDD[Vector], mypath: String = "/user/hduser/"): KMeansModel = {

    val sc = data.sparkContext
    val loop = new Breaks
    val loop1 = new Breaks
    var sqErrors: List[Double]= List()


    val initStartTime = System.nanoTime()
    var toggle: Boolean = false
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
        //Checkpoints are set up at every 10th % of the maxiterations and the model is stored in /user/hduser/["applicationId""iteration"]

        if (is10percent(iteration) == true) {
          val (minCost, bestRun) = costs.zipWithIndex.min
          val WSE = evaluate(bestRun, centers, data1)
          sqErrors = WSE::sqErrors
          toggle = true
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"------------------------------start  ---------------------------------------------------------")
          logInfo(s"Within Set Sum of Squared Errors = $WSE at iteration: $iteration")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          logInfo(s"----------------------------------------------------------------------------------------------")
          //trait compare
          val tempModel = new KMeansModel(centers(bestRun).map(_.vector))
          val savePath: String = mypath + sc.applicationId + "Iteration" + iteration
          logInfo(s"Model at iteration number: $iteration is saved in $savePath")
          tempModel.save(sc, savePath)
        }

        // Exit the learning process. Note: criteria are just set for test purposes. Need to estimate the criteria parameters after qualitative assesment.
        // Things to be decided: arraySize

        loop1.breakable {
          if (sc.checkRMRequest() == true && toggle == true) {
            val tempb: Boolean = sc.checkRMRequest()
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"RM Request = $tempb")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            //arraySize and WSErate directly infers to tolerance to errors. [Potential: can make this as input parameters in KMeans3 to preset expected quality in results before the
            val listSize = 3
            val WSErate = 0.95
            if (sqErrors.size >= listSize) {
              val WSE1 = sqErrors{0}
              val WSE2 = sqErrors{1}
              val WSE3 = sqErrors{2}


              if ((WSE1 / WSE2 > WSErate) && (WSE2 / WSE3 > WSErate)) {
                loop.break()
              }
              else {
                logInfo(s"-----------------------------------------------------------------------------------------------------")
                logInfo(s"The Within square errors rate still seems to be improving, hence can't stop the clustering process")
                logInfo(s"-----------------------------------------------------------------------------------------------------")
                loop1.break()
              }

            }
          }
          loop1.break()
        } //Breaking the loop
        }
      loop.break()
      }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9

    if (toggle == true) {
      logInfo(s"Iterations took " + "%.3f".format(iterationTimeInSeconds) + " seconds.")
      val loadPath: String = mypath + sc.applicationId + "Iteration" + iteration

      if (iteration == maxIterations) {
        logInfo(s"KMeans reached the max number of iterations: $maxIterations.")
        val loadPath: String = mypath + sc.applicationId + "Iteration" + iteration
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
            math.min(KMeans.pointCost(bcNewCenters.value(r), point), cost(r))
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
        ((r, KMeans.findClosest(bcCenters.value(r), p)._1), 1.0)
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
object KMeans3 {

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
    new KMeans3(k, maxIterations, runs, initializationMode)
      .run(data)



  }

  /**
   * Trains a k-means model using the given set of parameters.
   *
   */

  def train(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int,
             runs: Int,
             initializationMode: String): KMeansModel = {
    new KMeans3(k,runs, maxIterations, initializationMode).run(data)

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


