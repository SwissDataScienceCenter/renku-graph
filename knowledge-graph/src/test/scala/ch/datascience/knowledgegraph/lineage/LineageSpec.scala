/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class LineageSpec extends WordSpec with ScalaCheckPropertyChecks {

  type EitherLineage[Lineage] = Either[Throwable, Lineage]

  "from" should {

    "succeed if all the edges have nodes definitions" in {
      forAll(edgesSets) { edgesSet =>
        val nodesSet                                 = collectNodes(edgesSet)
        val Right(Lineage(actualEdges, actualNodes)) = Lineage.from[EitherLineage](edgesSet, nodesSet)

        actualEdges shouldBe edgesSet
        actualNodes shouldBe nodesSet
      }
    }

    "fail if there are edges with no nodes definitions" in {
      val edgesSet        = edgesSets.generateOne
      val nodesSet        = collectNodes(edgesSet)
      val nodeToBeMissing = Gen.oneOf(nodesSet.toList).generateOne

      val Left(exception) = Lineage.from[EitherLineage](edgesSet, nodesSet - nodeToBeMissing)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "There are edges with no nodes definitions"
    }

    "fail if there are orphan nodes" in {
      val edgesSet = edgesSets.generateOne
      val nodesSet = collectNodes(edgesSet) + nodes.generateOne

      val Left(exception) = Lineage.from[EitherLineage](edgesSet, nodesSet)

      exception            shouldBe an[IllegalArgumentException]
      exception.getMessage shouldBe "There are orphan nodes"
    }
  }

  private val edgesSets: Gen[Set[Edge]] = for {
    edgesNumber <- positiveInts()
    edgesSet    <- setOf[Edge](edges, edgesNumber)
  } yield edgesSet

  private def collectNodes(edges: Set[Edge]): Set[Node] =
    edges.foldLeft(Set.empty[Node]) { (nodes, edge) =>
      nodes + edge.source + edge.target
    }
}
