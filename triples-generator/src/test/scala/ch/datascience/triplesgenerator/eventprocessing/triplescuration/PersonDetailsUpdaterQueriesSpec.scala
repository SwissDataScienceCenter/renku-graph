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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Person
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.PersonDetailsUpdater.{prepareUpdates, Person => UpdaterPerson}
import io.circe.Json
import io.renku.jsonld.syntax._
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PersonDetailsUpdaterQueriesSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "prepareUpdates" should {

    "generate query changing person's name and removing label - case when email present" in {
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

        val name1Updated = names.generateOne

        val updates = prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(person1Id.value), Set(name1Updated), Set(email1))
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

    "generate query changing person's name and email and removing label - case when email removed" in {
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

        val name1Updated = names.generateOne

        val updates = prepareUpdates(
          Set(
            UpdaterPerson(ResourceId(person1Id.value), Set(name1Updated), Set.empty)
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
