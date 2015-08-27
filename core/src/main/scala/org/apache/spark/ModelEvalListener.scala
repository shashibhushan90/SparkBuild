package org.apache.spark


import org.apache.spark.scheduler.{SparkListenerRMRequest, SparkListener}


class ModelEvalListener extends SparkListener {

  var evalModel: Boolean = false

  override def onRMRequest(rmRequest: SparkListenerRMRequest): Boolean = {
    evalModel = true
    evalModel
  }

}