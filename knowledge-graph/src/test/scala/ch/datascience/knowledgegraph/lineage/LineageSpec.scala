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
import ch.datascience.generators.Generators._
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.{Edge, Lineage, Node}
import ch.datascience.rdfstore.entities.bundles.{prov, schema, wfprov}
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class LineageSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  type EitherLineage[Lineage] = Either[Throwable, Lineage]

  "from" should {

    "succeed if all the edges have nodes definitions" in {
      forAll(edgesSets) { edgesSet =>
        val nodesSet                                 = generateNodes(edgesSet)
        val Right(Lineage(actualEdges, actualNodes)) = Lineage.from[EitherLineage](edgesSet, nodesSet)

        actualEdges shouldBe edgesSet
        actualNodes shouldBe nodesSet
      }
    }

    "fail if there are edges with no nodes definitions" in {
      val edgesSet        = edgesSets.generateOne
      val nodesSet        = generateNodes(edgesSet)
      val nodeToBeMissing = Gen.oneOf(nodesSet.toList).generateOne

      val Left(exception) = Lineage.from[EitherLineage](edgesSet, nodesSet - nodeToBeMissing)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "There are edges with no nodes definitions"
    }

    "fail if there are orphan nodes" in {
      val edgesSet = edgesSets.generateOne
      val nodesSet = generateNodes(edgesSet) + entityNodes.generateOne

      val Left(exception) = Lineage.from[EitherLineage](edgesSet, nodesSet)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "There are orphan nodes"
    }
  }

  "singleWordType" should {

    s"return '${Node.SingleWordType.ProcessRun}' " +
      "if node contains the 'http://purl.org/wf4ever/wfprov#ProcessRun' type" in {
      val node = entityNodes.generateOne.copy(
        types = Set(
          (prov / "Activity").toString,
          (wfprov / "ProcessRun").toString
        ).map(Node.Type.apply)
      )

      node.singleWordType shouldBe Right(Node.SingleWordType.ProcessRun)
    }

    s"return '${Node.SingleWordType.File}' " +
      "if node contains the 'http://www.w3.org/ns/prov#Entity' type but not 'http://www.w3.org/ns/prov#Collection'" in {
      val node = entityNodes.generateOne.copy(
        types = Set(
          (prov / "Entity").toString,
          (wfprov / "Artifact").toString
        ).map(Node.Type.apply)
      )

      node.singleWordType shouldBe Right(Node.SingleWordType.File)
    }

    s"return '${Node.SingleWordType.Directory}' " +
      "if node contains the 'http://www.w3.org/ns/prov#Entity' and 'http://www.w3.org/ns/prov#Collection' types" in {
      val node = entityNodes.generateOne.copy(
        types = Set(
          (prov / "Entity").toString,
          (wfprov / "Artifact").toString,
          (prov / "Collection").toString
        ).map(Node.Type.apply)
      )

      node.singleWordType shouldBe Right(Node.SingleWordType.Directory)
    }

    "return an Exception there's no match to the given types" in {
      val types = Set(
        (wfprov / "Artifact").toString,
        (schema / "Dataset").toString
      )

      val Left(exception) = entityNodes.generateOne
        .copy(types = types.map(Node.Type.apply))
        .singleWordType

      exception.getMessage shouldBe s"${types.mkString(", ")} cannot be converted to a NodeType"
    }
  }
  "getNode" should {

    "return a Node if there is one with the given location" in {
      forAll { lineage: Lineage =>
        val node = Random.shuffle(lineage.nodes.toList).head

        lineage.getNode(node.location) shouldBe node.some
      }
    }

    "return None if no node are found with the given location" in {
      forAll { lineage: Lineage =>
        val location = nodeLocations.generateOne

        lineage.getNode(location) shouldBe None
      }
    }
  }

  private lazy val edgesSets: Gen[Set[Edge]] = for {
    edgesNumber <- positiveInts(max               = 20)
    edgesSet    <- setOf[Edge](edges, minElements = 1, maxElements = edgesNumber)
  } yield edgesSet

  private def generateNodes(edges: Set[Edge]): Set[Node] =
    edges.foldLeft(Set.empty[Node]) { (acc, edge) =>
      acc + entityNodes.generateOne.copy(location = edge.source) + entityNodes.generateOne.copy(location = edge.target)
    }
}
