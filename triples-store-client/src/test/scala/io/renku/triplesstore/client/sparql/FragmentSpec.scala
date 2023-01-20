/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore.client
package sparql

import TriplesStoreGenerators.{quads, triples}
import cats.Monoid
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class FragmentSpec extends AnyWordSpec with should.Matchers {

  "empty" should {

    "be a Fragment with an empty sparql" in {
      Fragment.empty.sparql shouldBe ""
    }
  }

  "monoid" should {

    "define empty as the Fragment.empty" in {
      Monoid[Fragment].empty shouldBe Fragment.empty
    }

    "define the combine method that puts sparqls from both Fragments into a single Fragment" in {
      val quad1 = quads.generateOne.asSparql
      val quad2 = quads.generateOne.asSparql

      List(quad1, quad2).combineAll shouldBe Fragment(s"${quad1.sparql}\n${quad2.sparql}")
    }
  }

  "show" should {

    "return the sparql property value" in {
      val fragment = Gen.oneOf(triples.map(_.asSparql), quads.map(_.asSparql)).generateOne
      fragment.show shouldBe fragment.sparql
    }
  }
}
