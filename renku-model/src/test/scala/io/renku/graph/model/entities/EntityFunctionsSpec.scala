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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.GraphModelGenerators.{personNameResourceId, renkuUrls}
import io.renku.graph.model.persons.Name
import io.renku.graph.model.{GraphClass, RenkuUrl}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EntityFunctionsSpec extends AnyWordSpec with should.Matchers {

  "there" should {
    "be an implicit instance of Contravariant for the EntityFunctions" in {

      def doSomeJsonLD(graph: GraphClass, value: String) = JsonLD.fromString(s"$graph -> $value")

      val functionsA = new EntityFunctions[A] {

        override val findAllPersons: A => Set[Person] = a =>
          Person
            .from(personNameResourceId.generateOne, Name(a.name), maybeOrcid = None)
            .fold(_ => fail("Cannot build Person"), Set(_))

        override val encoder: GraphClass => JsonLDEncoder[A] = { graph =>
          JsonLDEncoder.instance(a => doSomeJsonLD(graph, a.name))
        }
      }

      val functionsB: EntityFunctions[B] = functionsA.contramap(b => A(b.prop))

      val b = nonEmptyStrings().generateAs(B.apply)
      functionsB.findAllPersons(b).map(_.name.show) shouldBe Set(b.prop)

      val graph = Gen.oneOf(GraphClass.all).generateOne
      implicit val enc: JsonLDEncoder[B] = functionsB.encoder(graph)
      b.asJsonLD shouldBe doSomeJsonLD(graph, b.prop)
    }
  }

  case class A(name: String)
  case class B(prop: String)

  private implicit lazy val renkuUrl: RenkuUrl = renkuUrls.generateOne
}
