/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.users.Id
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.Person
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.PersonDetailsUpdater.{prepareUpdates, Person => UpdaterPerson}
import io.circe.Json
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class PersonDetailsUpdaterQueriesSpec extends WordSpec with InMemoryRdfStore {

  "prepareUpdates" should {

    "generate query changing person's name and removing label - case when email present" in {
      val name1     = names.generateOne
      val email1    = emails.generateOne
      val person1Id = Person.Id(Some(email1))
      val name2     = names.generateOne
      val email2    = emails.generateOne
      val person2Id = Person.Id(Some(email2))

      loadToStore(
        triples(List(Person(person1Id, name1), Person(person2Id, name2)))
      )

      findPersons should contain theSameElementsAs Set(
        (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
        (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
      )

      val name1Updated = names.generateOne

      val updates = prepareUpdates(
        Set(
          UpdaterPerson(Id(person1Id.value), Set(name1Updated), Set(email1))
        )
      )

      (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

      findPersons should contain theSameElementsAs Set(
        (person1Id.value, Some(name1Updated.value), Some(email1.value), None),
        (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
      )
    }

    "generate query changing person's name and email and removing label - case when email removed" in {
      val name1     = names.generateOne
      val email1    = emails.generateOne
      val person1Id = Person.Id(Some(email1))
      val name2     = names.generateOne
      val email2    = emails.generateOne
      val person2Id = Person.Id(Some(email2))

      loadToStore(
        triples(List(Person(person1Id, name1, Some(email1), None), Person(person2Id, name2, Some(email2), None)))
      )

      findPersons should contain theSameElementsAs Set(
        (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
        (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
      )

      val name1Updated = names.generateOne

      val updates = prepareUpdates(
        Set(
          UpdaterPerson(Id(person1Id.value), Set(name1Updated), Set.empty)
        )
      )

      (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

      findPersons should contain theSameElementsAs Set(
        (person1Id.value, Some(name1Updated.value), None, None),
        (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
      )
    }

    "generate query adding person's name and email - case when there's no name or email on the Person" in {
      val personId = Person.Id(Some(emails.generateOne))

      loadToStore(
        triples(
          List(
            Person(personId, names.generateOne, None, None)
              .remove("http://schema.org/name")
              .remove("http://www.w3.org/2000/01/rdf-schema#label")
          )
        )
      )

      findPersons should contain theSameElementsAs Set(
        (personId.value, None, None, None)
      )

      val name  = names.generateOne
      val email = emails.generateOne

      val updates = prepareUpdates(
        Set(
          UpdaterPerson(Id(personId.value), Set(name), Set(email))
        )
      )

      (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()

      findPersons should contain theSameElementsAs Set(
        (personId.value, Some(name.value), Some(email.value), None)
      )
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

  private implicit class JsonOps(json: Json) {
    import io.circe.optics.JsonPath._

    def remove(property: String): Json = root.obj.modify(_.remove(property))(json)
  }
}
