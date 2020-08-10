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

import PersonDetailsUpdater.{Person => UpdaterPerson}
import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.rdfstore.entities.bundles.renkuBaseUrl
import eu.timepit.refined.auto._
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "prepareUpdates" should {

    "generate query creating person with a name and email" in new TestCase {
      forAll { (name: Name, email: Email) =>
        findPersons shouldBe empty

        val personId = Person(name, email).asJsonLD.entityId.get

        val updates = updatesCreator.prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(personId), NonEmptyList.of(name), Set(email))
          )
        )

        (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (personId.value, Some(name.value), Some(email.value), None)
        )

        clearDataset()
      }
    }

    "generate query creating a Person with a name only" in new TestCase {

      val name     = userNames.generateOne
      val personId = Person(name).asJsonLD.entityId.get

      val updates = updatesCreator.prepareUpdates(
        Set(
          UpdaterPerson(ResourceId(personId), NonEmptyList.of(name), emails = Set.empty)
        )
      )

      (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

      val actual = findPersons
      actual should have size 1
      val (_, Some(actualName), None, None) = actual.head
      actualName shouldBe name.value
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

        val updates = updatesCreator.prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(person1Id), NonEmptyList.of(name1Updated), Set(email1))
          )
        )

        (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

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

        val updates = updatesCreator.prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(person1Id), NonEmptyList.of(name1Updated), emails = Set.empty)
          )
        )

        (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person1Id.value, Some(name1Updated.value), None, None),
          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
        )

        clearDataset()
      }
    }

    "generate query updating person's details with multiple names and emails " +
      "- case when person id is known" in new TestCase {
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

        val name1Updates  = nonEmptyList(userNames, minElements = 2).generateOne
        val email1Updates = nonEmptySet(userEmails, minElements = 2).generateOne

        val updates = updatesCreator.prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(person1Id), name1Updates, email1Updates)
          )
        )

        (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

        val results = findPersons
        results.filter(_._1 == person1Id.value).map(_._2) shouldBe name1Updates.map(v => Some(v.value)).toList.toSet
        results.filter(_._1 == person1Id.value).map(_._3) shouldBe email1Updates.map(v => Some(v.value))
        results.filter(_._1 == person2Id.value) shouldBe Set(
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
