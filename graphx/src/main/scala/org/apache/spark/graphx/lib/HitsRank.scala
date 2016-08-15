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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD


/**
 * Implements the HITS (also known as 'hubs and authorities') iterative graph algorithm
 * The algorithm accepts a focused connected graph of linked pages/documents as its input
 * and produces normalized (hub,authority) vertex rank tuple
 *
 * @see [[http://en.wikipedia.org/wiki/HITS_algorithm]]
 */
object HitsRank extends Logging {


  /**
   * Executes a given number of HITS algorithm iterations unless
   * convergence tolerance is achieved first.
   * <P>
   * Each iteration updates HITS (hub,authority) ranks pair for every vertex in a graph
   * The update routine has two phases:
   * <OL>
   * <LI> Update vertex Authority rank based on Hub ranks of the vertex incoming neighbors
   * <LI> Update vertex Hub rank based on Authority ranks of the vertex outgoing neighbors
   * </OL>
   * Each update phase normalizes the computed ranks
   * as is required for the algorithm convergence. L,,2,, norm is used in this implementation:
   * the norm choice affects convergence rate but not the result (up to a scale)
   * <P>
   * The HITS algorithm is highly similar to the Power-Method
   * used for iterative approximate computation of
   * the dominant eigen-vector(as in Principal Component Analysis).
   * Specifically, the HITS should converge to:
   * <OL>
   * <LI> Hub rank: principal unit eigen-vector of the matrix '''AA^T^'''
   * <LI> Authority rank: principal unit eigen-vector of the matrix '''A^T^A'''
   * </OL>
   * where '''A''' is the graph adjacency matrix.
   * <P>
   * Convergence rate of the HITS algorithm may be super-exponential in the worst-case scenarios.
   * It can be accelerated by using the Repeated Squaring technique.
   * This technique requires caching of the powers of matrix '''A'''
   * which is likely to be memory- and/or
   * communication-cost prohibitive for large graphs.
   * This HITS algorithm implementation is lean on caching
   * (only caches the previous and the current iteration's (Hub, Authority)
   * ranks and only for the duration of the iteration)
   * The Repeated Squaring technique is not used in this implementation
   * <P>
   *
   * @see [[http://www.math.cornell.edu/~mec/Winter2009/RalucaRemus/Lecture4/lecture4.html]]
   * @see [[https://en.wikipedia.org/wiki/Exponentiation_by_squaring]]
   * @param graph         the graph for which to compute the HITS ranks
   * @param initValue     initial value of HITS ranks to start iterating from (default is emptyRDD)
   * @param numIter       number of iterations of the main algorithm routine (default is 1)
   * @param convTolerance convergence tolerance(approximate Mean-Squared-Error, default is 0.0)
   * @tparam VD type of the graph vertex attribute object
   * @tparam ED type of the graph edge attribute object
   * @return transformed graph containing (Hub,Authority) HITS rank tuple at each vertex
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      initValue: RDD[(VertexId, (Double, Double))] = SparkContext.getOrCreate().emptyRDD,
      numIter: Int = 1,
      convTolerance: Double = 0.0): Graph[(Double, Double), ED] = {

    runWithExtraReturnInfo(graph, initValue, numIter, convTolerance)._1
  }

  /**
   * Extended output version of the run() method
   * In addition, returns a value object describing how HITS algorithm performed.
   * The value object is a map of [[org.apache.spark.graphx.lib.HitsAlgorithmMetric]]
   * keyed by the metric case Class(indicates metric uniqueness within the map,
   * enforced within this run() method)
   *
   * @param graph         the graph for which to compute the HITS ranks
   * @param initValue     initial value of HITS ranks to start iterating from (default is emptyRDD)
   * @param numIter       number of iterations of the main algorithm routine (default is 1)
   * @param convTolerance convergence tolerance(approximate Mean-Squared-Error, default is 0.0)
   * @tparam VD type of the graph vertex attribute object
   * @tparam ED type of the graph edge attribute object
   * @return HITS graph and value object with algorithm performance details
   */
  private [lib] def runWithExtraReturnInfo[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      initValue: RDD[(VertexId, (Double, Double))] = SparkContext.getOrCreate().emptyRDD,
      numIter: Int = 1,
      convTolerance: Double = 0.0): (Graph[(Double, Double), ED],
                                     Map[Class[_ <: HitsAlgorithmMetric], HitsAlgorithmMetric]) = {

    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ($numIter)")

    // the condition below guarantees that there will never be
    // a situation with zero norm of the ranks collection
    require(!graph.edges.isEmpty(), "Collection of edges in the input graph must be non-empty")

    val useConvergence: Boolean = convTolerance > 0.0

    // Initialize the HITS graph vertex with initial values
    var hitsGraph: Graph[(Double, Double), ED] = graph.
      outerJoinVertices(initValue)(
      (id, oldVal, initHitsVal) => initHitsVal.getOrElse((1.0, 1.0)))

    // materialize initial HITS values
    hitsGraph.vertices.foreachPartition(x => {})

    var convergence = false
    var extraAlgOutput = Map[Class[_ <: HitsAlgorithmMetric], HitsAlgorithmMetric]()

    // Perform a fixed number of iterations
    for (i <- 0 until numIter if !convergence) {
      // need to store reference for cache clean up at the end
      val prev = hitsGraph

      // compute new Authority rank by aggregation of the latest Hub ranks of the incoming neighbors
      // note: the authRanks will not contain nodes with zero in-degree
      val authRanks: VertexRDD[Double] = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _, TripletFields.Src).cache()


      // merge the Authority rank into the overall HITS rank graph
      // the missing zero in-degree nodes are set with 0.0 auth rank
      // such nodes do not affect and are not needed for the preceding normalization calculation
      hitsGraph = hitsGraph.outerJoinVertices(
        normalizeRanksUsingL2Norm(authRanks))(
        (id, oldHitsVal, newAuthVal) => (oldHitsVal._1, newAuthVal.getOrElse(0.0)))


      // compute new Hub rank by aggregation of the latest Authority ranks of the outgoing neighbors
      // note: the hubRanks will not contain nodes with zero out-degree
      val hubRanks = hitsGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _, TripletFields.Dst).cache()


      // merge the Hub rank into the overall HITS rank graph
      // the missing zero out-degree nodes are set with 0.0 hub rank
      // such nodes do not affect and are not needed for the preceding normalization calculation
      hitsGraph = hitsGraph.outerJoinVertices(
        normalizeRanksUsingL2Norm(hubRanks))(
        (id, oldHitsVal, newHubVal) => (newHubVal.getOrElse(0.0), oldHitsVal._2))

      hitsGraph.vertices.cache()

      // materialize the new HITS vertex values: allows to clean up after each iteration
      hitsGraph.vertices.foreachPartition(x => {})

      // lazy MSE error definition
      lazy val mseProxy = computeHitsMSEProxy(
        prev.vertices, hitsGraph.vertices)

      convergence = useConvergence && (mseProxy <= convTolerance)

      // log lazily
      logDebug("Mean-Square-Difference at HITS algorithm iteration #" + (i + 1) + " : " + mseProxy)

      // clean up cache
      authRanks.unpersist(false)
      hubRanks.unpersist(false)
      prev.unpersist(false)

      // just put the algIterationsNumber so far: it is cheap to provide
      extraAlgOutput += (classOf[HitsIterations] -> HitsIterations(i + 1))
    }

    // return final HITS rank with extra alg output

    (hitsGraph, extraAlgOutput)

  }

  /**
   * Computes the L2 norm of the difference between
   * previous and last HITS iteration rank values
   * This is a good convergence indicator, a proxy for MSE error
   * Expensive to compute - so should be used lazily
   *
   * @param prev previous HITS ranks
   * @param next current HITS ranks
   * @return single floating value of the L2 norm of the difference
   */
  def computeHitsMSEProxy(
      prev: VertexRDD[(Double, Double)],
      next: VertexRDD[(Double, Double)]): Double = {

    val errors: VertexRDD[(Double, Double)] = prev.innerJoin(next)(
      (vid, x, y) => (x._1 - y._1, x._2 - y._2))

    val er: (Double, Double) = errors.map(x => (x._2._1 * x._2._1, x._2._2 * x._2._2)).reduce(
      (x, y) => (x._1 + y._1, x._2 + y._2))

    math.sqrt(er._1 + er._2)
  }

  /**
   * Normalize ranks using L2 norm
   *
   * @param ranks rdd of vertex ranks
   * @return L2 normalized vertex ranks
   */
  private def normalizeRanksUsingL2Norm(ranks: VertexRDD[Double]): VertexRDD[Double] = {

    // compute the total square norm of the Hub rank collection
    // this operation is expensive but is required for convergence
    // it materializes input ranks (should be previously marked for caching by the caller)
    // the norm below is computed only once and is enclosed by the returned RDD
    val norm: Double = math.sqrt(ranks.map(x => x._2 * x._2).reduce(_ + _))

    if (norm > 0.0) ranks.mapValues(a => a / norm) else ranks

  }

}


/**
 * Defines a case-class family of HITS algorithm performance metrics
 *
 * @usecase runWithExtraInfo()
 */
abstract class HitsAlgorithmMetric


/**
 * Actual number of HITS algorithm iterations taken
 * @param iterNum number of iterations
 */
case class HitsIterations(iterNum: Int) extends HitsAlgorithmMetric


