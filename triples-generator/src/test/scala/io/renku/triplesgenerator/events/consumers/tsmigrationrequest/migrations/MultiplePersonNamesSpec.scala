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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personNames
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling.UpdateQueryMigration

class MultiplePersonNamesSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset {

  "query" should {

    "remove duplicate Person names" in {
      val person      = personEntities(withGitLabId).generateOne.to[entities.Person]
      val otherPerson = personEntities(withGitLabId).generateOne.to[entities.Person]

      upload(to = renkuDataset, person, otherPerson)

      val duplicateNames = personNames.generateNonEmptyList().toList.toSet
      duplicateNames foreach { name =>
        insert(to = renkuDataset, Triple(person.resourceId, schema / "name", name))
      }

      findNames(person.resourceId) shouldBe duplicateNames + person.name

      runUpdate(on = renkuDataset, MultiplePersonNames.query).unsafeRunSync() shouldBe ()

      findNames(person.resourceId)        should contain oneElementOf (duplicateNames + person.name)
      findNames(otherPerson.resourceId) shouldBe Set(otherPerson.name)
    }
  }

  "apply" should {
    "return an UpdateQueryMigration" in {
      implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      MultiplePersonNames[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findNames(id: persons.ResourceId): Set[persons.Name] =
    runSelect(on = renkuDataset,
              SparqlQuery.of("fetch person name",
                             Prefixes of schema -> "schema",
                             s"""SELECT ?name WHERE { <$id> a schema:Person; schema:name ?name }"""
              )
    ).unsafeRunSync()
      .map(row => persons.Name(row("name")))
      .toSet
}
