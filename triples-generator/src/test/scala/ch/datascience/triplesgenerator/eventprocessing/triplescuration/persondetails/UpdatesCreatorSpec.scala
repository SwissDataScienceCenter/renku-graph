/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles.renkuBaseUrl
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.{Person => UpdatedPerson}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "prepareUpdates" should {

    "generate query creating person with a name and email" in new TestCase {
      forAll { (name: Name, email: Email) =>
        findPersons shouldBe empty

        val personId = Person(name, email).asJsonLD.entityId.get

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          UpdatedPerson(ResourceId(personId), name, Some(email))
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        val persons = findPersons
        persons should contain theSameElementsAs Set(
          (personId.value, Some(name.value), Some(email.value), None)
        )

        clearDataset()
      }
    }

    "generate query creating a Person with a name only" in new TestCase {
      forAll { name: Name =>
        val personId = Person(name).asJsonLD.entityId.get

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          UpdatedPerson(ResourceId(personId), name, maybeEmail = None)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        val actual = findPersons
        actual should have size 1
        val (_, Some(actualName), None, None) = actual.head
        actualName shouldBe name.value
        clearDataset()
      }
    }

    "generate query updating person's name and removing label - case when person id is known" in new TestCase {
      forAll { (name1: Name, email1: Email, name2: Name, email2: Email) =>
        val person1Json = Person(name1, email1).asJsonLD
        val person1Id   = person1Json.entityId.get
        val person2Json = Person(name2, email2).asJsonLD
        val person2Id   = person2Json.entityId.get

        loadToStore(person1Json, person2Json)

        findPersons should contain theSameElementsAs Set(
          (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
        )

        val name1Updated = userNames.generateOne

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          UpdatedPerson(ResourceId(person1Id), name1Updated, Some(email1))
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person1Id.value, Some(name1Updated.value), Some(email1.value), None),
          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
        )

        clearDataset()
      }
    }

    "generate query updating person's name and removing email and label - case when person id is known" in new TestCase {
      forAll { (name1: Name, email1: Email, name2: Name, email2: Email) =>
        val person1Json = Person(name1, email1).asJsonLD
        val person1Id   = person1Json.entityId.get
        val person2Json = Person(name2, email2).asJsonLD
        val person2Id   = person2Json.entityId.get

        loadToStore(person1Json, person2Json)

        findPersons should contain theSameElementsAs Set(
          (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
        )

        val name1Updated = userNames.generateOne

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          UpdatedPerson(ResourceId(person1Id), name1Updated, maybeEmail = None)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person1Id.value, Some(name1Updated.value), None, None),
          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
        )

        clearDataset()
      }
    }

  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String])] =
    runQuery(s"""|SELECT ?id ?name ?email ?label
                 |WHERE {
                 |  ?id rdf:type schema:Person .
                 |  OPTIONAL { ?id schema:name ?name } .
                 |  OPTIONAL { ?id schema:email ?email } .
                 |  OPTIONAL { ?id rdfs:label ?label } .
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row.get("name"), row.get("email"), row.get("label")))
      .toSet

  private trait TestCase {
    val updatesCreator = new UpdatesCreator
  }
}
