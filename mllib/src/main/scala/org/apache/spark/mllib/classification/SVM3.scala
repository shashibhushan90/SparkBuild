package org.apache.spark.mllib.classification

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{GradientDescent3, SquaredL2Updater, HingeGradient}
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression.{LabeledPoint, GeneralizedLinearAlgorithm, GeneralizedLinearModel}
import org.apache.spark.mllib.util.{DataValidators, Loader, Saveable}
import org.apache.spark.rdd.RDD


class SVMModel3 (override val weights: Vector, override val intercept: Double) extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable
with Saveable with PMMLExportable {
  private var threshold: Option[Double] = Some(0.0)

  /**
   * :: Experimental ::
   * Sets the threshold that separates positive predictions from negative predictions. An example
   * with prediction score greater than or equal to this threshold is identified as an positive,
   * and negative otherwise. The default value is 0.0.
   * @since 1.3.0
   */
  @Experimental
  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  /**
   * :: Experimental ::
   * Returns the threshold (if any) used for converting raw prediction scores into 0/1 predictions.
   * @since 1.3.0
   */
  @Experimental
  def getThreshold: Option[Double] = threshold

  /**
   * :: Experimental ::
   * Clears the threshold so that `predict` will output raw prediction scores.
   * @since 1.0.0
   */
  @Experimental
  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
                                       dataMatrix: Vector,
                                       weightMatrix: Vector,
                                       intercept: Double) = {
    val margin = weightMatrix.toBreeze.dot(dataMatrix.toBreeze) + intercept
    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin
    }
  }

  /**
   * @since 1.3.0
   */
  override def save(sc: SparkContext, path: String): Unit = {
    GLMClassificationModel.SaveLoadV1_0.save(sc, path, this.getClass.getName,
      numFeatures = weights.size, numClasses = 2, weights, intercept, threshold)
  }

  override protected def formatVersion: String = "1.0"

  /**
   * @since 1.4.0
   */
  override def toString: String = {
    s"${super.toString}, numClasses = 2, threshold = ${threshold.getOrElse("None")}"
  }
}

object SVMModel3 extends Loader[SVMModel3] {

  /**
   * @since 1.3.0
   */
  override def load(sc: SparkContext, path: String): SVMModel3 = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    // Hard-code class name string in case it changes in the future
    val classNameV1_0 = "org.apache.spark.mllib.classification.SVMModel3"
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val (numFeatures, numClasses) = ClassificationModel.getNumFeaturesClasses(metadata)
        val data = GLMClassificationModel.SaveLoadV1_0.loadData(sc, path, classNameV1_0)
        val model = new SVMModel3(data.weights, data.intercept)
        assert(model.weights.size == numFeatures, s"SVMModel3.load with numFeatures=$numFeatures" +
          s" was given non-matching weights vector of size ${model.weights.size}")
        assert(numClasses == 2,
          s"SVMModel3.load was given numClasses=$numClasses but only supports 2 classes")
        data.threshold match {
          case Some(t) => model.setThreshold(t)
          case None => model.clearThreshold()
        }
        model
      case _ => throw new Exception(
        s"SVMModel3.load did not recognize model with (className, format version):" +
          s"($loadedClassName, $version).  Supported:\n" +
          s"  ($classNameV1_0, 1.0)")
    }


  }
}

/**
 * Train a Support Vector Machine (SVM) using Stochastic Gradient Descent. By default L2
 * regularization is used, which can be changed via [[SVMWithSGD. optimizer]].
 * NOTE: Labels used in SVM should be {0, 1}.
 */
class SVMWithSGD3 private (
                           private var stepSize: Double,
                           private var numIterations: Int,
                           private var regParam: Double,
                           private var miniBatchFraction: Double,
                           val testData: RDD[LabeledPoint])
  extends GeneralizedLinearAlgorithm[SVMModel3] with Serializable {

  private val gradient = new HingeGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent3(gradient, updater, testData)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
   * Construct a SVM object with default parameters: {stepSize: 1.0, numIterations: 100,
   * regParm: 0.01, miniBatchFraction: 1.0}.
   */
 //def this() = this(1.0, 100, 0.01, 1.0, testData)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new SVMModel3(weights, intercept)
  }
}

/**
 * Top-level methods for calling SVM. NOTE: Labels used in SVM should be {0, 1}.
 */
object SVMWithSGD3 {

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient. The weights used in
   * gradient descent are initialized using the initial weights provided.
   *
   * NOTE: Labels used in SVM should be {0, 1}.
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @param initialWeights Initial set of weights to be used. Array should be equal in size to
   *        the number of features in the data.
   * @since 0.8.0
   */
  def train(
             input: RDD[LabeledPoint],
             test: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double,
             initialWeights: Vector): SVMModel3 = {
    new SVMWithSGD3(stepSize, numIterations, regParam, miniBatchFraction, test)
      .run(input, initialWeights)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. Each iteration uses
   * `miniBatchFraction` fraction of the data to calculate the gradient.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @param stepSize Step size to be used for each iteration of gradient descent.
   * @param regParam Regularization parameter.
   * @param miniBatchFraction Fraction of data to be used per iteration.
   * @since 0.8.0
   */
  def train(
             input: RDD[LabeledPoint],
             test: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double,
             miniBatchFraction: Double): SVMModel3 = {
    new SVMWithSGD3(stepSize, numIterations, regParam, miniBatchFraction, test).run(input)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using the specified step size. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param stepSize Step size to be used for each iteration of Gradient Descent.
   * @param regParam Regularization parameter.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   * @since 0.8.0
   */
  def train(
             input: RDD[LabeledPoint],
             test: RDD[LabeledPoint],
             numIterations: Int,
             stepSize: Double,
             regParam: Double): SVMModel3 = {
    train(input, test, numIterations, stepSize, regParam, 1.0)
  }

  /**
   * Train a SVM model given an RDD of (label, features) pairs. We run a fixed number
   * of iterations of gradient descent using a step size of 1.0. We use the entire data set to
   * update the gradient in each iteration.
   * NOTE: Labels used in SVM should be {0, 1}
   *
   * @param input RDD of (label, array of features) pairs.
   * @param numIterations Number of iterations of gradient descent to run.
   * @return a SVMModel which has the weights and offset from training.
   * @since 0.8.0
   */
  def train(input: RDD[LabeledPoint], test: RDD[LabeledPoint], numIterations: Int): SVMModel3 = {
    train(input, test, numIterations, 1.0, 0.01, 1.0)
  }
}




