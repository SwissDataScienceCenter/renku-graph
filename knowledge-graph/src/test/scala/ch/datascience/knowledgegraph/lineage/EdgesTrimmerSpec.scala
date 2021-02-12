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

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.EdgeMap
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class EdgesTrimmerSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  /**
    *             i _
    *                 \
    *  a -- B -- c -- H -- j
    *  e _/   \_ d
    *    \_ F -- g
    *
    *   x -- Z -- y
    *
    *   trim on a should return B c d H j
    *   trim on c should return a B c e H j
    *   trim on d should return a B e
    *   trim on e should return B c d F g H j
    *   trim on g should return e F
    *   trim on i should return H j
    *   trim on j should return a B c e H i
    */

  "trim" should {
    val a = Location("a")
    val c = Location("c")
    val d = Location("d")
    val e = Location("e")
    val g = Location("g")
    val i = Location("i")
    val j = Location("j")
    val x = Location("x")
    val y = Location("y")
    val B = EntityId.of("B")
    val F = EntityId.of("F")
    val H = EntityId.of("H")
    val Z = EntityId.of("Z")

    val graph = Map[EntityId, (Set[Location], Set[Location])](
      B -> (Set(a, e), Set(c, d)),
      F -> (Set(e), Set(g)),
      H -> (Set(c, i), Set(j)),
      Z -> (Set(x), Set(y))
    )

    "return a B c d H j when looking for a" in new TestCase {
      edgesTrimmer.trim(graph, a) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        B -> (Set(a), Set(c, d)),
        H -> (Set(c), Set(j))
      ).pure[Try]
    }

    "return a B c e H j when looking for c" in new TestCase {
      edgesTrimmer.trim(graph, c) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        B -> (Set(a, e), Set(c)),
        H -> (Set(c), Set(j))
      ).pure[Try]
    }

    "return a B e when looking for d" in new TestCase {
      edgesTrimmer.trim(graph, d) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        B -> (Set(a, e), Set(d))
      ).pure[Try]
    }

    "return B c d F g H j when looking for e" in new TestCase {
      edgesTrimmer.trim(graph, e) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        B -> (Set(e), Set(c, d)),
        H -> (Set(c), Set(j)),
        F -> (Set(e), Set(g))
      ).pure[Try]
    }

    "return e F when looking for g" in new TestCase {
      edgesTrimmer.trim(graph, g) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        F -> (Set(e), Set(g))
      ).pure[Try]
    }

    "return H j when looking for i" in new TestCase {
      edgesTrimmer.trim(graph, i) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        H -> (Set(i), Set(j))
      ).pure[Try]
    }

    "return a B c e H i when looking for j" in new TestCase {
      edgesTrimmer.trim(graph, j) shouldBe Map[EntityId, (Set[Location], Set[Location])](
        B -> (Set(a, e), Set(c)),
        H -> (Set(c, i), Set(j))
      ).pure[Try]
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
