package org.apache.spark.mllib.classification

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD3
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.classification.SVMModel3
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


/**
 * @author srikumar
 */
object SVMBinaryClassifier{
  
  
  def train(training:RDD[LabeledPoint], test: RDD[LabeledPoint], sc:SparkContext):SVMModel3 = {

    //  Run training algorithm to build the model
    val numIterations = 100
    SVMWithSGD3.train(training, test, numIterations)
  }
}