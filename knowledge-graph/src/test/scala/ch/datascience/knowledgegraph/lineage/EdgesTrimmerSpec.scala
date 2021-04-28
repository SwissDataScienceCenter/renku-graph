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
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import io.renku.jsonld.EntityId
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant
import scala.util.Try

class EdgesTrimmerSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  /**             i _
    *                 \
    *  a -- B -- c -- H -- j
    *  e _/   \_ d
    *    \_ F -- g
    *  k __ L __/
    *
    *  x -- Z -- y
    *
    *   trim on a should return B c d H j
    *   trim on c should return a B c e H j
    *   trim on d should return a B e
    *   trim on e should return B c d F g H j
    *   trim on g should return e F (with assumption that F is more recent than L)
    *   trim on i should return H j
    *   trim on j should return a B c e H i
    *   trim on k should return nothing (with assumption that F is more recent than L)
    */

  "trim" should {
    val a = Location("a")
    val c = Location("c")
    val d = Location("d")
    val e = Location("e")
    val g = Location("g")
    val i = Location("i")
    val j = Location("j")
    val k = Location("k")
    val x = Location("x")
    val y = Location("y")
    val B = RunInfo(EntityId.of("B"), Instant.now())
    val F = RunInfo(EntityId.of("F"), B.date plusSeconds 60)
    val H = RunInfo(EntityId.of("H"), F.date plusSeconds 60)
    val L = RunInfo(EntityId.of("L"), F.date minusSeconds 60)
    val Z = RunInfo(EntityId.of("Z"), Instant.now())

    val graph: EdgeMap = Map(
      B -> (Set(a, e), Set(c, d)),
      F -> (Set(e), Set(g)),
      H -> (Set(c, i), Set(j)),
      L -> (Set(k), Set(g)),
      Z -> (Set(x), Set(y))
    )

    "return a B c d H j when looking for a" in {
      edgesTrimmer.trim(graph, a) shouldBe Map(
        B -> (Set(a), Set(c, d)),
        H -> (Set(c), Set(j))
      ).pure[Try]
    }

    "return a B c e H j when looking for c" in {
      edgesTrimmer.trim(graph, c) shouldBe Map(
        B -> (Set(a, e), Set(c)),
        H -> (Set(c), Set(j))
      ).pure[Try]
    }

    "return a B e when looking for d" in {
      edgesTrimmer.trim(graph, d) shouldBe Map(
        B -> (Set(a, e), Set(d))
      ).pure[Try]
    }

    "return B c d F g H j when looking for e" in {
      edgesTrimmer.trim(graph, e) shouldBe Map(
        B -> (Set(e), Set(c, d)),
        H -> (Set(c), Set(j)),
        F -> (Set(e), Set(g))
      ).pure[Try]
    }

    "return e F when looking for g" in {
      edgesTrimmer.trim(graph, g) shouldBe Map(
        F -> (Set(e), Set(g))
      ).pure[Try]
    }

    "return H j when looking for i" in {
      edgesTrimmer.trim(graph, i) shouldBe Map(
        H -> (Set(i), Set(j))
      ).pure[Try]
    }

    "return a B c e H i when looking for j" in {
      edgesTrimmer.trim(graph, j) shouldBe Map(
        B -> (Set(a, e), Set(c)),
        H -> (Set(c, i), Set(j))
      ).pure[Try]
    }

    "return nothing when looking for k" in {
      edgesTrimmer.trim(graph, k) shouldBe Map.empty.pure[Try]
    }

    "return nothing if the given location does not exists in the given edges map" in {
      edgesTrimmer.trim(
        lineages.generateOne.toEdgesMap,
        nodeLocations.generateOne
      ) shouldBe Map.empty.pure[Try]
    }
  }

  private lazy val edgesTrimmer = new EdgesTrimmerImpl[Try]()
}
