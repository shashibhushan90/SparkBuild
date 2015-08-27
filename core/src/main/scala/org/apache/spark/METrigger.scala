package org.apache.spark

/**
 * Created by shashi_ibm on 24-Aug-15.
 */
trait METrigger {
  var RMRequest: Boolean = false

  def setMETrigger(state: Boolean): Unit ={
    RMRequest = state
  }

  def getMETrigger(): Boolean ={
    return RMRequest
  }
}
