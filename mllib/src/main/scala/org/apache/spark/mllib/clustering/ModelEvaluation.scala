// Trait for Model Evaluation
package org.apache.spark.mllib.clustering

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

trait ModelEvaluation {
	def evaluate(
       bestRun: Int,
       centers: Array[Array[VectorWithNorm]],
       data1: RDD[Vector]):Double = 
       {
             val dynamicModel = new KMeansModel(centers(bestRun).map(_.vector))
             val WSSSE = dynamicModel.computeCost(data1)
 	           return WSSSE             
      } 
      // def stopApplication()   
}