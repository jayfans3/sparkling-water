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

package org.apache.spark.h2o.converters

import org.apache.spark.TaskContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.backends.external.{ExternalReadConverterContext, ExternalWriteConverterContext}
import org.apache.spark.h2o.backends.internal.{InternalReadConverterContext, InternalWriteConverterContext}
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.sql.types._
import water.{ExternalFrameHandler, DKV, Key}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._


private[converters] trait ConverterUtils {


  def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update()
  }


  def finalizeFrame[T](keyName: String,
                       res: Array[Long],
                       colTypes: Array[Byte],
                       colDomains: Array[Array[String]] = null):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colTypes)
    fr
  }

  /**
    * Gets frame for specified key or none if that frame does not exist
 *
    * @param keyName key of the requested frame
    * @return option containing frame or none
    */
  def getFrameOrNone(keyName: String): Option[H2OFrame] = {
    // Fetch cached frame from DKV
    val frameVal = DKV.get(keyName)

    if(frameVal != null){
      Some(new H2OFrame(frameVal.get.asInstanceOf[Frame]))
    }else{
      None
    }
  }

  /**
    * Converts the RDD to H2O Frame using specified conversion function
    *
    * @param hc H2O context
    * @param rdd rdd to convert
    * @param keyName key of the resulting frame
    * @param colNames names of the columns in the H2O Frame
    * @param vecTypes types of the vectors in the H2O Frame
    * @param func conversion function - the function takes parameters needed extra by specific transformations
    *             and returns function which does the general transformation
    * @tparam T type of RDD to convert
    * @return H2O Frame
    */
  def convert[T](hc: H2OContext, rdd : RDD[T], keyName: String, colNames: Array[String], vecTypes: Array[Byte],
                 func: ( (String, Array[Byte], Option[immutable.Map[Int, NodeDesc]]) => (TaskContext, Iterator[T]) => (Int, Long))) = {
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, colNames)

    // prepare rdd and required metadata based on the used backend
    val (preparedRDD, uploadPlan) = if(hc.getConf.runsInExternalClusterMode){
       val res = ExternalWriteConverterContext.scheduleUpload[T](rdd)
      (res._1, Some(res._2))
    }else{
      (rdd, None)
    }

    val rows = hc.sparkContext.runJob(preparedRDD, func(keyName, vecTypes, uploadPlan)) // eager, not lazy, evaluation
    val res = new Array[Long](preparedRDD.partitions.length)
    rows.foreach { case (cidx,  nrows) => res(cidx) = nrows }
    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }
}

object ConverterUtils {
  def getWriteConverterContext(uploadPlan: Option[immutable.Map[Int, NodeDesc]], partitionId: Int): WriteConverterContext = {
    val converterContext = if (uploadPlan.isDefined) {
      new ExternalWriteConverterContext(uploadPlan.get.get(partitionId).get)
    } else {
      new InternalWriteConverterContext()
    }
    converterContext
  }

  def getReadConverterContext(isExternalBackend: Boolean, keyName: String, chksLocation: Map[Int, NodeDesc],
                              types: Option[Array[Byte]], chunkIdx: Int): ReadConverterContext = {
    val converterContext = if (isExternalBackend) {
      new ExternalReadConverterContext(keyName, chunkIdx, chksLocation(chunkIdx), types.get)
    } else {
      new InternalReadConverterContext(keyName, chunkIdx)
    }
    converterContext
  }

  def getIterator[T](isExternalBackend: Boolean, iterator: Iterator[T]): Iterator[T] = {
    if (isExternalBackend) {
      val rows = new ListBuffer[T]()
      while (iterator.hasNext) {
        rows += iterator.next()
      }
      rows.iterator
    } else {
      iterator
    }
  }


  def prepareExpectedTypes[T: TypeTag](isExternalBackend: Boolean, types: Array[T]): Option[Array[Byte]] =
    if(!isExternalBackend){
      None
    }else {
      typeOf[T] match {
        case t if t =:= typeOf[DataType] =>
          Some(types.map {
            case ByteType => ExternalFrameHandler.T_INTEGER
            case ShortType => ExternalFrameHandler.T_INTEGER
            case IntegerType => ExternalFrameHandler.T_INTEGER
            case LongType => ExternalFrameHandler.T_INTEGER
            case FloatType => ExternalFrameHandler.T_INTEGER
            case DoubleType => ExternalFrameHandler.T_DOUBLE
            case BooleanType => ExternalFrameHandler.T_INTEGER
            case StringType => ExternalFrameHandler.T_STRING
            case TimestampType => ExternalFrameHandler.T_INTEGER
          })
        case t if t =:= typeOf[Class[_]] =>
          Some(types.map {
            case q if q == classOf[Integer] => ExternalFrameHandler.T_INTEGER
            case q if q == classOf[java.lang.Long] => ExternalFrameHandler.T_INTEGER
            case q if q == classOf[java.lang.Double] => ExternalFrameHandler.T_DOUBLE
            case q if q == classOf[java.lang.Float] => ExternalFrameHandler.T_INTEGER
            case q if q == classOf[java.lang.Boolean] => ExternalFrameHandler.T_INTEGER
            case q if q == classOf[String] => ExternalFrameHandler.T_STRING
          })
      }
    }

}

