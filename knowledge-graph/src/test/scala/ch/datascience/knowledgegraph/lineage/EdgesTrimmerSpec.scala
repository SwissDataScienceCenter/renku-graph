/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.knowledgegraph.lineage

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.{Edge, EdgeMap, Lineage}
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Random, Try}

class EdgesTrimmerSpec extends WordSpec with ScalaCheckPropertyChecks {

  "trim" should {

    "return the given edges map if there is only one graph connected to the given location" in new TestCase {
      forAll { lineage: Lineage =>
        val location = Random.shuffle(lineage.locationNodes.toList).head.location
        val edgesMap = lineage.toEdgesMap

        edgesTrimmer.trim(edgesMap, location) shouldBe edgesMap.pure[Try]
      }
    }

    "return an edges map without graphs not connected to the given location" in new TestCase {
      forAll { (lineage1: Lineage, lineage2: Lineage) =>
        val location                = Random.shuffle(lineage1.locationNodes.toList).head.location
        val Lineage(edges1, nodes1) = lineage1
        val Lineage(edges2, nodes2) = lineage2
        val mergedLineages          = Lineage.from[Try](edges1 ++ edges2, nodes1 ++ nodes2).fold(throw _, identity)

        edgesTrimmer.trim(mergedLineages.toEdgesMap, location) shouldBe lineage1.toEdgesMap.pure[Try]
      }
    }

    "return an edges map containing all graphs connected to the given file but the ones not connected" in new TestCase {
      forAll { (lineage1: Lineage, lineage2: Lineage, lineage3: Lineage) =>
        val location                   = Random.shuffle(lineage1.locationNodes.toList).head.location
        val Lineage(edges1, nodes1)    = lineage1
        val Lineage(edges2, nodes2)    = lineage2
        val Lineage(edges3, nodes3)    = lineage3
        val commonLocationNode         = Random.shuffle(lineage1.locationNodes.toList).head
        val someLineage3RunPlanNode    = Random.shuffle(lineage3.processRunNodes.toList).head
        val lineage1And3ConnectionEdge = Edge(commonLocationNode.location, someLineage3RunPlanNode.location)
        val mergedLineages: Lineage =
          Lineage
            .from[Try](edges1 ++ edges2 ++ edges3 + lineage1And3ConnectionEdge, nodes1 ++ nodes2 ++ nodes3)
            .fold(throw _, identity)

        edgesTrimmer.trim(mergedLineages.toEdgesMap, location) shouldBe Lineage
          .from[Try](edges1 ++ edges3 + lineage1And3ConnectionEdge, nodes1 ++ nodes3)
          .map(_.toEdgesMap)
      }
    }

    "return all edges sharing the same location" in new TestCase {
      forAll { lineage: Lineage =>
        val commonLocation         = Random.shuffle(lineage.locationNodes.toList).head.location
        val additionalRunPlanNodes = processRunNodes.generateNonEmptyList(minElements = 2).toList.toSet
        val additionalEdges = additionalRunPlanNodes.map { runPlanNode =>
          if (Random.nextBoolean()) Edge(runPlanNode.location, commonLocation)
          else Edge(commonLocation, runPlanNode.location)
        }
        val lineageWithSharedLocation = Lineage
          .from[Try](lineage.edges ++ additionalEdges, lineage.nodes ++ additionalRunPlanNodes)
          .fold(throw _, identity)
        val edgesMap = lineageWithSharedLocation.toEdgesMap

        edgesTrimmer.trim(
          edgesMap,
          location = Random.shuffle(lineage.locationNodes.toList).head.location
        ) shouldBe edgesMap.pure[Try]
      }
    }

    "return an empty edges map if the given location does not exists in the given edges map" in new TestCase {
      edgesTrimmer.trim(
        lineages.generateOne.toEdgesMap,
        nodeLocations.generateOne
      ) shouldBe (Map.empty: EdgeMap).pure[Try]
    }
  }

  private trait TestCase {
    val edgesTrimmer = new EdgesTrimmerImpl[Try]()
  }
}
