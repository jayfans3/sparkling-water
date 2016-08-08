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

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import water.fvec.{Frame, FrameUtils}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Convert H2OFrame into an RDD (lazily)
 */


private[converters]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private (@transient val hc: H2OContext,
                                                       @transient val frame: T,
                                                       val colNames: Array[String])
  extends RDD[A](hc.sparkContext, Nil){

  // Get column names before building an RDD
  def this(h2oContext: H2OContext, fr : T) = this(h2oContext, fr, ReflectionUtils.names[A])

  // Check that H2OFrame & given Scala type are compatible
  if (colNames.length > 1) {
    colNames.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }

  val isExternalBackend = hc.getConf.runsInExternalClusterMode
  val types = ReflectionUtils.types[A](colNames)
  // Create new types list which describes expected types in a way external H2O backend can use it. This list
  // contains types in a format same for H2ODataFrame and H2ORDD
  val expectedTypes = ConverterUtils.prepareExpectedTypes(isExternalBackend, types)

  @transient val jc = implicitly[ClassTag[A]].runtimeClass
  @transient val cs = jc.getConstructors
  @transient val ccr = cs.collectFirst(
        { case c if c.getParameterTypes.length == colNames.length => c })
        .getOrElse( {
                      throw new IllegalArgumentException(
                        s"Constructor must take exactly ${colNames.length} args")})

  // Fields outside the compute method are accessed in the driver node and therefore also by H2O node in client mode on
  // that node
  /** Cache frame key to get H2OFrame from the K/V store */
  val keyName = frame._key.toString
  val numCols = frame.numCols()
  // Chunk locations helps us to determine the node which really has the data we needs
  val chksLocation = FrameUtils.getChunksLocations(frame)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    val con = ConverterUtils.getReadConverterContext(isExternalBackend, keyName, chksLocation, expectedTypes, split.index)

    val iterator = new Iterator[A]{
      def hasNext: Boolean = con.hasNext

      val jc = implicitly[ClassTag[A]].runtimeClass
      val cs = jc.getConstructors
      val ccr = cs.collectFirst({
                case c if c.getParameterTypes.length == colNames.length => c
              })
        .getOrElse({
            throw new IllegalArgumentException(
                  s"Constructor must take exactly ${colNames.length} args")
      })

      def next(): A = {
        val data = new Array[Option[Any]](numCols)

        (0 until numCols).foreach{ idx =>
          val value = if (con.isNA(idx)) None
          else types(idx) match {
            case q if q == classOf[Integer]           => Some(con.getInt(idx))
            case q if q == classOf[java.lang.Long]    => Some(con.getLong(idx))
            case q if q == classOf[java.lang.Double]  => Some(con.getDouble(idx))
            case q if q == classOf[java.lang.Float]   => Some(con.getFloat(idx))
            case q if q == classOf[java.lang.Boolean] => Some(con.getBoolean(idx))
            case q if q == classOf[String] => Option(con.getString(idx))
            case _ => None
          }
          data(idx) = value
        }

        con.increaseRowIdx()
        ccr.newInstance(data:_*).asInstanceOf[A]
      }
    }

    ConverterUtils.getIterator[A](isExternalBackend, iterator)
  }

  /** Pass through an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    val num = frame.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }

}
