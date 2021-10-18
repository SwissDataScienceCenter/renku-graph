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

package io.renku.knowledgegraph.lineage

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import io.renku.jsonld.EntityId
import io.renku.knowledgegraph.lineage.LineageGenerators._
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.knowledgegraph.lineage.model._
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
    *  m -- N -- o -- Q -- r
    *         \_ p _/
    *
    *   trim on a should return B c d H j
    *   trim on c should return a B c e H j
    *   trim on d should return a B e
    *   trim on e should return B c d F g H j
    *   trim on g should return e F (with assumption that F is more recent than L)
    *   trim on i should return H j
    *   trim on j should return a B c e H i
    *   trim on k should return nothing (with assumption that F is more recent than L)
    *   trim on m should return m N o p Q r
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
    val m = Location("m")
    val o = Location("o")
    val p = Location("p")
    val r = Location("r")
    val B = ExecutionInfo(EntityId.of("B"), Instant.now())
    val F = ExecutionInfo(EntityId.of("F"), B.date.value plusSeconds 60)
    val H = ExecutionInfo(EntityId.of("H"), F.date.value plusSeconds 60)
    val L = ExecutionInfo(EntityId.of("L"), F.date.value minusSeconds 60)
    val N = ExecutionInfo(EntityId.of("N"), Instant.now())
    val Q = ExecutionInfo(EntityId.of("Q"), Instant.now())

    val graph: EdgeMap = Map(
      B -> (Set(a, e), Set(c, d)),
      F -> (Set(e), Set(g)),
      H -> (Set(c, i), Set(j)),
      L -> (Set(k), Set(g)),
      N -> (Set(m), Set(o, p)),
      Q -> (Set(o, p), Set(r))
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

    "return m N o p Q r when looking for m - diamond case" in {
      edgesTrimmer.trim(graph, m) shouldBe Map(
        N -> (Set(m), Set(o, p)),
        Q -> (Set(o, p), Set(r))
      ).pure[Try]
    }

    "return m N o p Q r when looking for r - diamond case" in {
      edgesTrimmer.trim(graph, r) shouldBe Map(
        N -> (Set(m), Set(o, p)),
        Q -> (Set(o, p), Set(r))
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
