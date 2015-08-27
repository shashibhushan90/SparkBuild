package org.apache.spark.mllib.classification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.SVMModel3
import org.apache.spark.mllib.regression.LabeledPoint


/**
 * Hello world!
 *
 */
class OneVsAll extends SVMMultiClassClassifier {
  
  val classes = (0 to 9).toList.map(_.toString)
  var models:List[(String,SVMModel3)] = null
  
  def train(input_rdd:RDD[String], test_rdd:RDD[LabeledPoint], sc:SparkContext) {
    
   
    val storageLevel =
      if (sc.getLocalProperty("sc.use.tachyon") != null && sc.getLocalProperty("sc.use.tachyon").equals("1")) {
        println("MOVE SIG FOR GREAT JUSTICE --> ALL YOUR MEMORY ARE BELONG TO TACHYON")
        StorageLevel.OFF_HEAP
      } else {
        StorageLevel.MEMORY_ONLY
      }
    
    models = classes.map { digit =>
      val rdd_digit = DataTransformer.transform(input_rdd, Option(digit)).persist(storageLevel) 
      val model = SVMBinaryClassifier.train(rdd_digit,test_rdd, sc)
      (digit, model)
    }
    
  }
  
  def predict(input_rdd:RDD[String]):Option[RDD[(Double,Double)]] = {
    
    if(models == null)
      return None
      
     val predicFunc = (point:LabeledPoint)  => {
        val scoreAndDigit = models.map {
          case (digit, model) =>
            val score = model.predict(point.features)
            (score, digit.toDouble)
        }.maxBy(_._1)
        (scoreAndDigit._2, point.label)
    }
     
   Option(DataTransformer.transform(input_rdd, None).map{ x=> predicFunc(x)})
  }
}