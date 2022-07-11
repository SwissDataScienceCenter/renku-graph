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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.persondetails

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personNames
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.jsonld.syntax._
import io.renku.graph.model.{entities, persons}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGPersonFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset {

  "find" should {

    "find a Person by its resourceId" in new TestCase {
      val person = personEntities().generateOne.to[entities.Person]

      upload(to = renkuDataset, person)

      finder.find(person).unsafeRunSync() shouldBe Some(person)
    }

    "pick-up one of the Person names if there multiple" in new TestCase {
      val person = personEntities().generateOne.to[entities.Person]

      upload(to = renkuDataset, person)

      val duplicateNames = personNames.generateNonEmptyList().toList.toSet
      duplicateNames foreach { name =>
        insert(to = renkuDataset, Triple(person.resourceId.asEntityId, schema / "name", name))
      }
      findNames(person) shouldBe duplicateNames + person.name

      val Some(found) = finder.find(person).unsafeRunSync()

      found.resourceId       shouldBe person.resourceId
      Set(found.name)          should contain oneElementOf (duplicateNames + person.name)
      found.maybeEmail       shouldBe person.maybeEmail
      found.maybeAffiliation shouldBe person.maybeAffiliation
    }

    "return no Person if it doesn't exist" in new TestCase {
      finder
        .find(personEntities().generateOne.to[entities.Person])
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new KGPersonFinderImpl[IO](renkuDSConnectionInfo)
  }

  private def findNames(id: entities.Person): Set[persons.Name] =
    runSelect(
      on = renkuDataset,
      SparqlQuery.of(
        "fetch person name",
        Prefixes of schema -> "schema",
        s"""|SELECT ?name 
            |WHERE { 
            |  ${id.resourceId.showAs[RdfResource]} schema:name ?name
            |}""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => persons.Name(row("name")))
      .toSet
}
