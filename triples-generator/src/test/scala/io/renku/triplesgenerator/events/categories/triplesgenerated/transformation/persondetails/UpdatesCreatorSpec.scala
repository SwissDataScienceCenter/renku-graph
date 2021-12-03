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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.persondetails

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  import UpdatesCreator._

  "preparePreDataUpdates" should {

    "generate queries which delete person's name, email and affiliation " +
      "in case all of them were changed" in {

        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(name = userNames.generateOne,
                                         maybeEmail = userEmails.generateSome,
                                         maybeAffiliation = userAffiliations.generateSome
        )

        loadToStore(kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           kgPerson.name.value.some,
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.gitLabId.value.some
          )
        )

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

        queries.runAll.unsafeRunSync()

        findPersons shouldBe Set((kgPerson.resourceId.value, None, None, None, kgPerson.gitLabId.value.some))
      }

    "generate queries which delete person's name, email and affiliation " +
      "in case they are removed" in {

        val Some(kgPerson) = personEntities(withGitLabId, withEmail)
          .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
          .generateOne
          .toMaybe[entities.Person.WithGitLabId]
        val mergedPerson = kgPerson.copy(maybeEmail = None, maybeAffiliation = None)

        loadToStore(kgPerson)

        findPersons shouldBe Set(
          (kgPerson.resourceId.value,
           kgPerson.name.value.some,
           kgPerson.maybeEmail.map(_.value),
           kgPerson.maybeAffiliation.map(_.value),
           kgPerson.gitLabId.value.some
          )
        )

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

        queries.runAll.unsafeRunSync()

        findPersons shouldBe Set(
          (kgPerson.resourceId.value, Some(kgPerson.name.value), None, None, kgPerson.gitLabId.value.some)
        )
      }

    "generate no queries when person's name, email and affiliation are the same" in {

      val kgPerson = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
        .generateOne
        .to[entities.Person]

      preparePreDataUpdates(kgPerson, kgPerson).isEmpty shouldBe true
    }
  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String], Option[Int])] =
    runQuery(s"""|SELECT ?id ?name ?email ?affiliation ?sameAsId ?gitlabId
                 |WHERE {
                 |  ?id a schema:Person .
                 |  OPTIONAL { ?id schema:name ?name } .
                 |  OPTIONAL { ?id schema:email ?email } .
                 |  OPTIONAL { ?id schema:affiliation ?affiliation } .
                 |  OPTIONAL { ?id schema:sameAs ?sameAsId.
                 |             ?sameAsId a schema:URL;
                 |                       schema:additionalType 'GitLab';
                 |                       schema:identifier ?gitlabId.
                 |  }
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row =>
        (row("id"), row.get("name"), row.get("email"), row.get("affiliation"), row.get("gitlabId").map(_.toInt))
      )
      .toSet
}
