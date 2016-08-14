/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import org.scalactic.{Equality, TolerantNumerics}

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._

class HitsRankSuite extends SparkFunSuite with LocalSparkContext {

  type Hits = (VertexId, (Double,Double))

  /**
   * Utility function for scaling of HITS rank to match a pair of expected HITS ranks.
   * Makes it easier to compare scaled ranks with non-canonically scaled expectations
   * @param expectedHub expected HITS rank to scale Hub ranks to
   * @param expectedAuth expected HITS ranks to scale Authority ranks to
   * @param ranks set of HITS ranks to scale
   * @return scaled set of HITS ranks
   */
  def scaleRankToMatchExpectedElements(expectedHub: Hits, expectedAuth: Hits, ranks: Iterable[Hits]): Iterable[Hits] = {

    assert(expectedHub._2._1 > 0.0, s"Expected Hub rank in ($expectedHub) should be positive for well-defined rank scaling")
    assert(expectedAuth._2._2 > 0.0, s"Expected Authority rank in ($expectedAuth) should be positive for well-defined rank scaling")

    val actualHubs  = ranks.filter( x => (x._1 == expectedHub._1))
    val actualAuths = ranks.filter( x => (x._1 == expectedAuth._1))

    assert(actualHubs.size == 1, s"Expected a single vertex in ($actualHubs) with ID matching expected Hub($expectedHub)")
    assert(actualAuths.size == 1, s"Expected a single vertex in ($actualAuths) with ID matching expected Authority($expectedAuth)")

    val actualHub: Hits = actualHubs.toSeq(0)
    val actualAuth: Hits = actualAuths.toSeq(0)

    assert(actualHub._2._1 > 0.0, s"Actual Hub rank in ($actualHub) should be positive for well-defined rank scaling")
    assert(actualAuth._2._2 > 0.0, s"Actual Authority rank in ($actualAuth) should be positive for well-defined rank scaling")

    val hubScale = expectedHub._2._1 / actualHub._2._1
    val authScale = expectedAuth._2._2 / actualAuth._2._2

    ranks.map(x => (x._1, (x._2._1 * hubScale, x._2._2 * authScale)))
  }

  /**
   * Tolerant Equality comparator of HITS ranks
   * @param tolerance double-precision tolerance
   * @return true/false equality comparison
   */
  def tolerantHitsRankEquality(tolerance: Double): Equality[Hits] = {
    new Equality[Hits] {
      implicit val deq: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(tolerance)
      def areEqual(a: Hits, b: Any): Boolean = {
        b match {
          case ((vid, (hub,auth))) => (a._1 == vid && a._2._1 === hub && a._2._2 === auth)
          case _ => false
        }
      }
      override def toString: String = s"tolerantHitsRankEquality($tolerance)"
    }
  }

  /**
   * Tolerant Equality comparator of sets of HITS ranks
   * @param tolerance double-precision tolerance
   * @return true/false equality comparison
   */
  def tolerantHitsRankCollectionEquality(tolerance: Double): Equality[Array[Hits]] = {
    new Equality[Array[Hits]]  {
      implicit val heq: Equality[Hits]  = tolerantHitsRankEquality(tolerance)
      def areEqual(a: Array[Hits], b: Any): Boolean = {
        b match {
          case s: Array[Hits] => {a.size == s.size && a.zip[Hits, Hits, Array[(Hits, Hits)]](s).filterNot(
            a => heq.areEqual(a._1,a._2) ).isEmpty}
          case _ => false
        }
      }
      override def toString: String = s"tolerantHitsRankCollectionEquality($tolerance)"
    }
  }

  /**
   * Runs the HITS test and returns the result after performing basic asserts
   * @param graph input graph
   * @param expectedRanks expected HITS ranks
   * @param numIter number of iterations
   * @param tolerance numeric double precision tolerance
   * @param algAsserts extra asserts on algorithm performance
   * @tparam VD vertex property type
   * @tparam ED edge property type
   * @return HITS results
   */
  def runHitsTestAndPerformBasicAsserts[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
      expectedRanks: Seq[Hits], numIter: Int, tolerance: Double,
      algAsserts: Map[String, Any => Boolean] = Map[String, Any => Boolean]()): Seq[Hits] = {

    implicit val hitsEq: Equality[Array[Hits]] = tolerantHitsRankCollectionEquality(math.max(0.0000001, tolerance))

    val expectedHub: Hits = expectedRanks.find( x => (x._2._1 > 0.0)).getOrElse[Hits]((0L, (0.0,0.0)))
    val expectedAuth: Hits = expectedRanks.find( x => (x._2._2 > 0.0)).getOrElse[Hits]((0L, (0.0,0.0)))

    val resultsExtra = HitsRank.runWithExtraReturnInfo(graph, numIter = numIter, convTolerance = 0.99*tolerance)
    val results = resultsExtra._1.vertices.collect()
    val extra: Map[String, Any] = resultsExtra._2

    val scaledResults = scaleRankToMatchExpectedElements(expectedHub, expectedAuth, results).toSeq

    assert(expectedRanks.toArray[Hits].sorted[Hits] === scaledResults.toArray[Hits].sorted[Hits])

    for ((key,value) <- extra){
      if (algAsserts.contains(key)) assert(algAsserts(key).apply(value), s"Failed test assert keyed by ($key)")
    }

    results.toSeq
  }

  /**
   * Need this ordering of HITS collections to implement
   * custom collection equality check via zipping
   */
  implicit val HitsOrdering = new Ordering[Hits] {
    override def compare(x: Hits, y: Hits): Int = {
      x._1.compareTo(y._1)
    }
  }

  test("HITS Rank: symmetric 2-node case") {

    withSpark { sc =>
      // simple 2-node symmetric graph
      val graph = Graph.fromEdgeTuples[Int](
        sc.parallelize[(VertexId, VertexId)](
          Seq((1L, 2L), (2L, 1L))), 1)
      // expect fully symmetric ranks
      val expectedRanks: Seq[Hits] = Seq(
        (1L, (1.0, 1.0)), (2L, (1.0, 1.0)))

      runHitsTestAndPerformBasicAsserts(graph, expectedRanks, 1, 0.0)

    }
  }


  test("HITS Rank: asymmetric 2-node case") {

    withSpark { sc =>
      // simple 2-node asymmetric Hub->Authority graph
      val graph = Graph.fromEdgeTuples[Int](
        sc.parallelize[(VertexId, VertexId)](
          Seq((1L, 2L))), 1)
      // expect fully asymmetric ranks
      val expectedRanks: Seq[Hits] = Seq(
        (1L, (1.0, 0.0)), (2L, (0.0, 1.0)))

      runHitsTestAndPerformBasicAsserts(graph, expectedRanks, 1, 0.0)

    }
  }

  test("HITS Rank: asymmetric 1-Hub-2-Auth tree case") {

    withSpark { sc =>
      // simple tree with 1 hub and 2 children authorities
      val graph = Graph.fromEdgeTuples[Int](
        sc.parallelize[(VertexId, VertexId)](
          Seq((1L, 2L), (1L, 3L))), 1)
      // expect equal Authority ranks for Authority nodes
      val expectedRanks: Seq[Hits] = Seq(
        (1L, (1.0, 0.0)), (2L, (0.0, 1.0/2.0)), (3L, (0.0, 1.0/2.0)))

      runHitsTestAndPerformBasicAsserts(graph, expectedRanks, 1, 0.0)

    }
  }

  test("HITS Rank: asymmetric 2-Hub-1-Auth case") {

    withSpark { sc =>
      // simple graph with 2 hubs sharing 1 authority
      val graph = Graph.fromEdgeTuples[Int](
        sc.parallelize[(VertexId, VertexId)](
          Seq((1L, 3L), (2L, 3L))), 1)
      // expect equal Hub ranks for Hub nodes
      val expectedRanks: Seq[Hits] = Seq(
        (1L, (1.0/2.0, 0.0)), (2L, (1.0/2.0, 0.0)), (3L, (0.0, 1.0)))

      runHitsTestAndPerformBasicAsserts(graph, expectedRanks, 1, 0.0)

    }
  }


  test("HITS Rank: 2 disconnected asymmetric sub-graphs") {

    withSpark { sc =>
      // 2 simple disconnected graphs:
      // 1st a simple Hub -> Authority pair
      // 2nd is a tree of 1 Hub and 2 Authorities
      val graph = Graph.fromEdgeTuples[Int](
        sc.parallelize[(VertexId, VertexId)](
          Seq((1L, 2L), (3L, 4L), (3L, 5L))), 1)
      // the large hub kills the small: this is the dominant principal-component property of HITS
      val expectedRanks: Seq[Hits] = Seq(
        (1L, (0.0, 0.0)), (2L, (0.0, 0.0)),
        (3L, (1.0, 0.0)), (4L, (0.0, 1.0/2.0)), (5L, (0.0, 1.0/2.0)))

      def iterCheck: Any => Boolean = {
        x => x match {
          case v: Int => v < 10
          case _ => false
        }
      }

      // will assert that actually fewer iterations were performed to achieve convergence
      runHitsTestAndPerformBasicAsserts(graph, expectedRanks, 10, 0.01,
        Map[String, Any => Boolean](HitsRank.AlgIterationsNumber -> iterCheck))

    }
  }

}
