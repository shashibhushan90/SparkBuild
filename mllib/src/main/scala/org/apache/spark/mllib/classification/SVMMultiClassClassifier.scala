package org.apache.spark.mllib.classification

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.io.Serializable

trait ModelEvaluation {
  def evaluate():Double
}
/**
 * @author srikumar
 */
abstract class SVMMultiClassClassifier extends Serializable {
 
   def train(input_rdd:RDD[String], test_rdd:RDD[LabeledPoint], sc:SparkContext):Unit
   def predict(input_rdd:RDD[String]):Option[RDD[(Double,Double)]]
   
}