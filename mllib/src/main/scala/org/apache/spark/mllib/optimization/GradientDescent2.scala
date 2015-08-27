/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.optimization


import org.apache.spark.mllib.classification.{SVMWithSGD2, SVMModel2}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.annotation.{Experimental, DeveloperApi}
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import util.control.Breaks._

import scala.util.control.Breaks

/**
 * Class used to solve an optimization problem using Gradient Descent.
 * @param gradient Gradient function to be used.
 * @param updater Updater to be used to update weights after every iteration.
 */
class GradientDescent2 private[mllib] (private var gradient: Gradient, private var updater: Updater, testData: RDD[LabeledPoint])
  extends Optimizer with Logging {

  private var stepSize: Double = 1.0
  private var numIterations: Int = 100
  private var regParam: Double = 0.0
  private var miniBatchFraction: Double = 1.0

  /**
   * Set the initial step size of SGD for the first step. Default 1.0.
   * In subsequent steps, the step size will decrease with stepSize/sqrt(t)
   */
  def setStepSize(step: Double): this.type = {
    this.stepSize = step
    this
  }


  /**
   * :: Experimental ::
   * Set fraction of data to be used for each SGD iteration.
   * Default 1.0 (corresponding to deterministic/classical gradient descent)
   */
  @Experimental
  def setMiniBatchFraction(fraction: Double): this.type = {
    this.miniBatchFraction = fraction
    this
  }

  /**
   * Set the number of iterations for SGD. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.numIterations = iters
    this
  }

  /**
   * Set the regularization parameter. Default 0.0.
   */
  def setRegParam(regParam: Double): this.type = {
    this.regParam = regParam
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for SGD.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }


  /**
   * Set the updater function to actually perform a gradient step in a given direction.
   * The updater is responsible to perform the update from the regularization term as well,
   * and therefore determines what kind or regularization is used, if any.
   */
  def setUpdater(updater: Updater): this.type = {
    this.updater = updater
    this
  }



  /**
   * :: DeveloperApi ::
   * Runs gradient descent on the given training data.
   * @param data training data
   * @param initialWeights initial weights
   * @return solution vector
   */
  @DeveloperApi
  def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = GradientDescent2.runMiniBatchSGD(
      data,
      testData,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFraction,
      initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run gradient descent.
 */
@DeveloperApi
object GradientDescent2 extends Logging {
  /**
   * Run stochastic gradient descent (SGD) in parallel using mini batches.
   * In each iteration, we sample a subset (fraction miniBatchFraction) of the total data
   * in order to compute a gradient estimate.
   * Sampling, and averaging the subgradients over this subset is performed using one standard
   * spark map-reduce in each iteration.
   *
   *
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the
   *         stochastic loss computed for every iteration.
   */

  var pathArray: List[String] = List()

  def is10percent(iteration: Int, numIterations: Int): Boolean = {
    val mylist = List(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0)
    val iterationInterval = (numIterations/10).toDouble
    val temp = (iteration/(iterationInterval))

    if(mylist.contains(temp)) {
      return true
    }
    else {
      return false
    }
  }
  def is5percent(iteration: Int, numIterations: Int): Boolean = {
    val mylist = List(0.5,1.0,2.5,2.0,2.5,3.0,3.5,4.0,4.5,5.0,5.5,6.0,6.5,7.0,7.5,8.0,8.5,9.0,9.5,10.0)
    val iterationInterval = (numIterations/10).toDouble
    val temp = (iteration/(iterationInterval))

    if(mylist.contains(temp)) {
      return true
    }
    else {
      return false
    }
  }

  def calculateAreaMeasures(pathArray: List[String], arrayPosition: Int, testData: RDD[LabeledPoint] ): List[Double] = {
    val loadModel = SVMModel2.load(SparkContext.getOrCreate(), pathArray{arrayPosition})
    val scoreAndLabels = testData.map { point =>
      val score = loadModel.predict(point.features)
      (score, point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    val auPR = metrics.areaUnderPR()
    List(auROC, auPR)
  }
  def calculateFThreshold(pathArray: List[String], arrayPosition: Int, testData: RDD[LabeledPoint] ) = {
    val loadModel = SVMModel2.load(SparkContext.getOrCreate(), pathArray{arrayPosition})
    val scoreAndLabels = testData.map { point =>
      val score = loadModel.predict(point.features)
      (score, point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val fThreshold = metrics.fMeasureByThreshold()
    val temp = fThreshold.collect().toList
    temp
  }



  def runMiniBatchSGD(
                       data: RDD[(Double, Vector)],
                       testData: RDD[LabeledPoint],
                       gradient: Gradient,
                       updater: Updater,
                       stepSize: Double,
                       numIterations: Int,
                       regParam: Double,
                       miniBatchFraction: Double,
                       initialWeights: Vector): (Vector, Array[Double]) = {

    val stochasticLossHistory = new ArrayBuffer[Double](numIterations)
    var trigME: Boolean = false
    val sc = SparkContext.getOrCreate()
    var trigFivePercent: Boolean = false
    var AuROCList: List[Double] = List(0.0)
    var AuPRList: List[Double] = List(0.0)
    var FThresholdList: List[Any] = List(0.0, 0.0)


    val numExamples = data.count()

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {
      logWarning("GradientDescent2.runMiniBatchSGD returning initial weights, no data found")
      return (initialWeights, stochasticLossHistory.toArray)
    }

    if (numExamples * miniBatchFraction < 1) {
      logWarning("The miniBatchFraction is too small")
    }

    // Initialize weights as a column vector
    var weights = Vectors.dense(initialWeights.toArray)
    val n = weights.size

    /**
     * For the first iteration, the regVal will be initialized as sum of weight squares
     * if it's L2 updater; for L1 updater, the same logic is followed.
     */
    var regVal = updater.compute(
      weights, Vectors.dense(new Array[Double](weights.size)), 0, 1, regParam)._2


    breakable {
      for (i <- 1 to numIterations) {

        val bcWeights = data.context.broadcast(weights)
        // Sample a subset (fraction miniBatchFraction) of the total data
        // compute and sum up the subgradients on this subset (this is one map-reduce)
        val (gradientSum, lossSum, miniBatchSize) = data.sample(false, miniBatchFraction, 42 + i)
          .treeAggregate((BDV.zeros[Double](n), 0.0, 0L))(
            seqOp = (c, v) => {
              // c: (grad, loss, count), v: (label, features)
              val l = gradient.compute(v._2, v._1, bcWeights.value, Vectors.fromBreeze(c._1))
              (c._1, c._2 + l, c._3 + 1)
            },
            combOp = (c1, c2) => {
              // c: (grad, loss, count)
              (c1._1 += c2._1, c1._2 + c2._2, c1._3 + c2._3)
            })



        if (miniBatchSize > 0) {
          /**
           * NOTE(Xinghao): lossSum is computed using the weights from the previous iteration
           * and regVal is the regularization value computed in the previous iteration as well.
           */
          stochasticLossHistory.append(lossSum / miniBatchSize + regVal)
          val update = updater.compute(
            weights, Vectors.fromBreeze(gradientSum / miniBatchSize.toDouble), stepSize, i, regParam)
          weights = update._1
          regVal = update._2

          val savePath: String = "/user/hduser/SVMModels/" + "iteration" + i.toString

          if (trigFivePercent == true && is5percent(i, numIterations) == true){
            pathArray = savePath :: pathArray
            val tempSize = stochasticLossHistory.size
            val tempArray = stochasticLossHistory.toArray
            val tempModel = new SVMModel2(weights, tempArray {
              tempSize - 1
            })
            tempModel.save(sc, savePath)
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"Check pointing SVM application at gradient descent iteration: $i")
            logInfo(s"Model at iteration number $i is stored at $savePath")
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"---------------------------------------------------------------------------------------------")
          }
          else if (is10percent(i, numIterations) == true && trigFivePercent == false) {
            pathArray = savePath :: pathArray
            val tempSize = stochasticLossHistory.size
            val tempArray = stochasticLossHistory.toArray
            val tempModel = new SVMModel2(weights, tempArray {
              tempSize - 1
            })
            tempModel.save(sc, savePath)
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"Check pointing SVM application at gradient descent iteration: $i")
            logInfo(s"Model at iteration number $i is stored at $savePath")
            logInfo(s"---------------------------------------------------------------------------------------------")
            logInfo(s"---------------------------------------------------------------------------------------------")
          }

          /* To be deleted in the final version*/
          if (i % 30 == 0) {
            trigME = true
          }

          if (sc.checkRMRequest() == true && trigME == true && trigFivePercent == false) {
            val areaMeasures2 = calculateAreaMeasures(pathArray, 2, testData)
            AuROCList = areaMeasures2(0) :: AuROCList
            AuPRList = areaMeasures2(1) :: AuPRList
            val areaMeasures1 = calculateAreaMeasures(pathArray, 1, testData)
            AuROCList = areaMeasures1(0) :: AuROCList
            AuPRList = areaMeasures1(1) :: AuPRList
            val areaMeasures0 = calculateAreaMeasures(pathArray, 0, testData)
            AuROCList = areaMeasures0(0) :: AuROCList
            AuPRList = areaMeasures0(1) :: AuPRList
            val fMeasure2 = calculateFThreshold(pathArray, 2, testData)
            FThresholdList = fMeasure2(1) :: FThresholdList
            FThresholdList = fMeasure2(0) :: FThresholdList
            val fMeasure1 = calculateFThreshold(pathArray, 1, testData)
            FThresholdList = fMeasure1(1) :: FThresholdList
            FThresholdList = fMeasure1(0) :: FThresholdList
            val fMeasure0 = calculateFThreshold(pathArray, 0, testData)
            FThresholdList = fMeasure0(1) :: FThresholdList
            FThresholdList = fMeasure0(0) :: FThresholdList
            AuROCList = AuROCList.dropRight(1)
            AuPRList = AuPRList.dropRight(1)
            FThresholdList = FThresholdList.dropRight(2)

            trigFivePercent = true

            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"Area under ROC for the last three checkpoints: $AuROCList")
            logInfo(s"Area under Precision Recall for the last three checkpoints: $AuPRList")
            logInfo(s"F Measure by Threshold values for the last three iterations: $FThresholdList")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          }   else if (sc.checkRMRequest() == true && trigME == true && trigFivePercent == true) {
            val areaMeasures0 = calculateAreaMeasures(pathArray, 0, testData)
            AuROCList = areaMeasures0(0) :: AuROCList
            AuPRList = areaMeasures0(1) :: AuPRList
            val fMeasure0 = calculateFThreshold(pathArray, 0, testData)
            FThresholdList = fMeasure0(1) :: FThresholdList
            FThresholdList = fMeasure0(0) :: FThresholdList

            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"Area under ROC for the last three checkpoints: $AuROCList")
            logInfo(s"Area under Precision Recall for the last three checkpoints: $AuPRList")
            logInfo(s"F Measure by Threshold values for the last three iterations: $FThresholdList")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logInfo(s"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
          }

          if(trigFivePercent == true) {
            if (AuROCList(2) / AuROCList(1) > 1.05 && AuROCList(1) / AuROCList(0) > 1.05) {
              logInfo(s"Exiting gradient descent at iteration: $i ")
              break()
            }
            else if (AuPRList(2) / AuPRList(1) > 1.05 && AuPRList(1) / AuPRList(0) > 1.05) {
              logInfo(s"Exiting gradient descent at iteration: $i ")
              break()
            }
            else {
              logInfo(s"Continuing Gradient Descent optimization. Model performance seems to be improving at iteration: $i ")
            }
          }
        }

        else {
          logWarning(s"Iteration ($i/$numIterations). The size of sampled batch is zero")
        }
      }
    }


    logInfo("GradientDescent2.runMiniBatchSGD finished. Last 10 stochastic losses %s".format(
      stochasticLossHistory.takeRight(10).mkString(", ")))
    (weights, stochasticLossHistory.toArray)

  }
}
