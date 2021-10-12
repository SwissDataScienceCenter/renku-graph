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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package persondetails

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.rdfstore.InMemoryRdfStore
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  import UpdatesCreator._

  "prepareUpdates" should {

    "generate queries which delete person's name, email, affiliation and gitLabId " +
      "in case all of them were changed" in {

        val kgPerson = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
          .generateOne
          .to[entities.Person]
        val mergedPerson = kgPerson.copy(name = userNames.generateOne,
                                         maybeEmail = userEmails.generateSome,
                                         maybeAffiliation = userAffiliations.generateSome,
                                         maybeGitLabId = userGitLabIds.generateSome
        )

        loadToStore(kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           Some(kgPerson.name.value),
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.maybeGitLabId.map(_.value)
          )
        )

        val queries = prepareUpdates(kgPerson, mergedPerson)

        queries.runAll.unsafeRunSync()

        findPersons shouldBe Set((kgPerson.resourceId.value, None, None, None, None))
      }

    "generate queries which delete person's name, email, affiliation and gitLabId " +
      "in case they are removed" in {

        val kgPerson = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
          .generateOne
          .to[entities.Person]
        val mergedPerson = kgPerson.copy(maybeEmail = None, maybeAffiliation = None, maybeGitLabId = None)

        loadToStore(kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           Some(kgPerson.name.value),
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.maybeGitLabId.map(_.value)
          )
        )

        val queries = prepareUpdates(kgPerson, mergedPerson)

        queries.runAll.unsafeRunSync()

        findPersons shouldBe Set((kgPerson.resourceId.value, Some(kgPerson.name.value), None, None, None))
      }

    "generate no queries when person's name, email, affiliation and gitLabId are the same" in {

      val kgPerson = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
        .generateOne
        .to[entities.Person]

      prepareUpdates(kgPerson, kgPerson).isEmpty shouldBe true
    }
  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String], Option[Int])] =
    runQuery(s"""|SELECT ?id ?name ?email ?affiliation ?sameAsId ?gitlabId
                 |WHERE {
                 |  ?id a schema:Person .
                 |  OPTIONAL { ?id schema:name ?name } .
                 |  OPTIONAL { ?id schema:email ?email } .
                 |  OPTIONAL { ?id schema:affiliation ?affiliation } .
                 |  OPTIONAL { ?id schema:sameAs ?sameAsId } .
                 |  OPTIONAL { ?sameAsId schema:identifier ?gitlabId;
                 |                       a schema:URL;
                 |                       schema:additionalType 'GitLab'
                 |  }
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row =>
        (row("id"), row.get("name"), row.get("email"), row.get("affiliation"), row.get("gitlabId").map(_.toInt))
      )
      .toSet
}
