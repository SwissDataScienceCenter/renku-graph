/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.data.OptionT

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.{Edge, Lineage}
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Random, Try}

class LineageDataCuratorSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "curate" should {

    "return the lineage if there is only one graph connected to the given file" in new TestCase {
      forAll { lineage: Lineage =>
        val location = Random.shuffle(lineage.nodes.toList).head.location

        lineageDataCurator.curate(lineage, location) shouldBe OptionT.some[Try](lineage)
      }
    }

    "return the lineage without graphs not connected to the given file" in new TestCase {
      forAll { (lineage1: Lineage, lineage2: Lineage) =>
        val location                = Random.shuffle(lineage1.nodes.toList).head.location
        val Lineage(edges1, nodes1) = lineage1
        val Lineage(edges2, nodes2) = lineage2
        val mergedLineages          = Lineage.from[Try](edges1 ++ edges2, nodes1 ++ nodes2).fold(throw _, identity)

        lineageDataCurator.curate(mergedLineages, location) shouldBe OptionT.some[Try](lineage1)
      }
    }

    "return the lineage if there are multiple graphs connected to the given file" in new TestCase {
      forAll { (lineage1: Lineage, lineage2: Lineage, lineage3: Lineage) =>
        val location                = Random.shuffle(lineage1.nodes.toList).head.location
        val Lineage(edges1, nodes1) = lineage1
        val Lineage(edges2, nodes2) = lineage2
        val Lineage(edges3, nodes3) = lineage3
        val commonNode              = Random.shuffle(lineage1.nodes.toList).head
        val additionalLineage3Node  = Random.shuffle(lineage3.nodes.toList).head
        val additionalLineage3Edge  = Edge(commonNode.location, additionalLineage3Node.location)
        val mergedLineages =
          Lineage
            .from[Try](edges1 ++ edges2 ++ edges3 + additionalLineage3Edge,
                       nodes1 ++ nodes2 ++ nodes3 + additionalLineage3Node
            )
            .fold(throw _, identity)

        lineageDataCurator.curate(mergedLineages, location) shouldBe OptionT.liftF(
          Lineage.from[Try](edges1 ++ edges3 + additionalLineage3Edge, nodes1 ++ nodes3 + additionalLineage3Node)
        )
      }
    }

    "find all edges sharing the same location" in new TestCase {
      forAll { lineage: Lineage =>
        val commonLocation  = Random.shuffle(lineage.nodes.toList).head.location
        val additionalNodes = nodes.generateNonEmptyList(minElements = 2).toList.toSet
        val additionalEdges = additionalNodes.map { n =>
          if (Random.nextBoolean()) Edge(n.location, commonLocation)
          else Edge(commonLocation, n.location)
        }
        val lineageWithSharedLocation = Lineage
          .from[Try](lineage.edges ++ additionalEdges, lineage.nodes ++ additionalNodes)
          .fold(throw _, identity)

        lineageDataCurator.curate(
          lineageWithSharedLocation,
          location = Random.shuffle(lineage.nodes.toList).head.location
        ) shouldBe OptionT.some[Try](lineageWithSharedLocation)
      }
    }

    "return None if the file path is not found in the lineage" in new TestCase {
      lineageDataCurator.curate(
        lineages.generateOne,
        nodeLocations.generateOne
      ) shouldBe OptionT.none[Try, Lineage]
    }
  }

  private trait TestCase {
    val lineageDataCurator = new LineageDataCuratorImpl[Try]()
  }
}
