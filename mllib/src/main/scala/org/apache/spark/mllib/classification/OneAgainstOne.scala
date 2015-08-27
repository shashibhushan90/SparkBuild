package org.apache.spark.mllib.classification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.SVMModel3
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Hello world!
 *
 */
class OneAgainstOne extends SVMMultiClassClassifier {

  val classes = (0 to 9).toList.map(_.toString).combinations(2).toList
  var classifiers:List[((String,String),SVMModel3)] = null
  
  def train(input_rdd:RDD[String], test_digit: RDD[LabeledPoint], sc:SparkContext) = {
    
    val storageLevel =
      if (sc.getLocalProperty("sc.use.tachyon") != null && sc.getLocalProperty("sc.use.tachyon").equals("1")) {
        StorageLevel.OFF_HEAP
      } else {
        StorageLevel.MEMORY_ONLY
      }
    
    classifiers = classes.map{case List(d1,d2) => 
      val rdd_digit = DataTransformer.transform(input_rdd.filter{ x => x.startsWith(d1) || x.startsWith(d2)}, Option(d1)).persist(storageLevel)
      val model = SVMBinaryClassifier.train(rdd_digit, test_digit, sc)
      ((d1,d2) -> model)
      }
  }
  
  def predict(input_rdd:RDD[String]):Option[RDD[(Double,Double)]] = {
   
    if(classifiers == null){
      println("classifiers are null")
      return None
    }
      
      
     val predicFunc = (point:LabeledPoint) => {
        val digit = classifiers.map {
          case ((d1,d2), model) =>
            val score = model.predict(point.features)
            if (score == 1.0) d1; else d2
        }.groupBy(identity).mapValues(_.size).maxBy(_._2)
        (digit._1.toDouble, point.label)
    }
     
    Option(DataTransformer.transform(input_rdd, None).map{ x=> predicFunc(x)})
  }
  
}