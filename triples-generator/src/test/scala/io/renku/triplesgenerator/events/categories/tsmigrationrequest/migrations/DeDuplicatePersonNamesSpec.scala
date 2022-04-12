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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personNames
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling.UpdateQueryMigration
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DeDuplicatePersonNamesSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  "query" should {

    "remove duplicate Person names" in {
      val person              = personEntities(withGitLabId).generateOne
      val personDuplicateName = person.copy(name = personNames.generateOne).to[entities.Person]
      val otherPerson         = personEntities(withGitLabId).generateOne.to[entities.Person]

      loadToStore(person.to[entities.Person], personDuplicateName, otherPerson)

      findNames(personDuplicateName.resourceId) shouldBe Set(person.name, personDuplicateName.name)

      runUpdate(DeDuplicatePersonNames.query).unsafeRunSync() shouldBe ()

      findNames(personDuplicateName.resourceId) should (be(Set(person.name)) or be(Set(personDuplicateName.name)))
      findNames(otherPerson.resourceId)       shouldBe Set(otherPerson.name)
    }
  }

  "apply" should {
    "return an UpdateQueryMigration" in {
      implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
      DeDuplicatePersonNames[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findNames(id: persons.ResourceId): Set[persons.Name] =
    runQuery(s"""SELECT ?name WHERE { <$id> a schema:Person; schema:name ?name }""")
      .unsafeRunSync()
      .map(row => persons.Name(row("name")))
      .toSet
}
