/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.files.lineage

import LineageGenerators._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.{Activity, Entity}
import model.{Edge, Lineage, Node}
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

  "Node.Type.fromEntityTypes" should {

    "return 'ProcessRun' if node contains Activity types" in {
      Node.Type.fromEntityTypes(Activity.entityTypes.toList.map(_.show).toSet) shouldBe Right(Node.Type.ProcessRun)
    }

    "return 'File' if node contains File Entity types" in {
      Node.Type.fromEntityTypes(Entity.fileEntityTypes.toList.map(_.show).toSet) shouldBe Right(Node.Type.File)
    }

    "return 'Directory' if node contains File Entity types" in {
      Node.Type.fromEntityTypes(Entity.folderEntityTypes.toList.map(_.show).toSet) shouldBe Right(Node.Type.Directory)
    }

    "return an Exception there's no match to the given types" in {
      val types = Set(schema / "Dataset", schema / "Project").map(_.show)

      val Left(exception) = Node.Type.fromEntityTypes(types)

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
    edgesNumber <- positiveInts(max = 20)
    edgesSet    <- setOf[Edge](edges, min = 1, max = edgesNumber)
  } yield edgesSet

  private def generateNodes(edges: Set[Edge]): Set[Node] =
    edges.foldLeft(Set.empty[Node]) { (acc, edge) =>
      acc + entityNodes.generateOne.copy(location = edge.source) + entityNodes.generateOne.copy(location = edge.target)
    }
}
