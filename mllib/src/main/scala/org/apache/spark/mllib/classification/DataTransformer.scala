package org.apache.spark.mllib.classification



import scala.io.Source
import java.io.File
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}


/**
 * @author srikumar
 * This code is a mirror of MLUtils.loadLibSVMFile except for the ability to select the digit 
 *   
 */
object DataTransformer {
 
  
  private def createLabeledPoint(line:String,digit:Option[String]):(Double,Array[Int],Array[Double]) = {
    var label:Double = 0.0
    var point:LabeledPoint = null
    
    label = digit match {
      case Some(digit) => if(line.startsWith(digit)) 1.0 else 0.0
      case None => line.split(' ').head.toDouble
    }
    
    val tokens = line.substring(1).split(' ')
    val splits = tokens.filter(_.nonEmpty)
                            .map {item =>
                                  val idxAndVal = item.split(':')
                                  val index = idxAndVal(0).toInt - 1
                                  val value = idxAndVal(1).toDouble/255
                                  (index,value)}
    val (indices,values) = splits.unzip
    return (label,indices.toArray,values.toArray)
  }
  
  def transform(input_rdd:RDD[String],digit:Option[String]):RDD[LabeledPoint] = {
    
    
    val parsed = input_rdd.filter{ line => !(line.isEmpty() || line.startsWith("#"))}
                              .map{ line => createLabeledPoint(line,digit) }
    //val numFeatures = parsed.map{ case (label,indices,values) => indices.lastOption.getOrElse(0)}.reduce(math.max)+1
    
    val numFeatures = 784                          
    
    parsed.map{case (label,indices,values) => new LabeledPoint(label, Vectors.sparse(numFeatures,indices,values))}
  }
    
}