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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, JsonLD, Property}
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  import UpdatesCreator._

  "preparePreDataUpdates" should {

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

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

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

        val queries = preparePreDataUpdates(kgPerson, mergedPerson)

        queries.runAll.unsafeRunSync()

        findPersons shouldBe Set((kgPerson.resourceId.value, Some(kgPerson.name.value), None, None, None))
      }

    "generate no queries when person's name, email, affiliation and gitLabId are the same" in {

      val kgPerson = personEntities(withGitLabId, withEmail)
        .map(_.copy(maybeAffiliation = userAffiliations.generateSome))
        .generateOne
        .to[entities.Person]

      preparePreDataUpdates(kgPerson, kgPerson).isEmpty shouldBe true
    }
  }

  "preparePostDataUpdates" should {

    "return no queries when the given person has no email" in {
      val person = personEntities(maybeEmails = withoutEmail).generateOne.to[entities.Person]
      preparePostDataUpdates(person).isEmpty shouldBe true
    }

    "return queries doing nothing when there's no person with the given email in KG " in {
      val person = personEntities(maybeEmails = withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      findPersons shouldBe Set(toResultRow(person))

      val queries = preparePostDataUpdates(personEntities(maybeEmails = withEmail).generateOne.to[entities.Person])

      queries.runAll.unsafeRunSync()

      findPersons shouldBe Set(toResultRow(person))
    }

    "return queries doing nothing when there's only one person with the given email in KG" in {
      val person = personEntities(maybeEmails = withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      findPersons shouldBe Set(toResultRow(person))

      val queries = preparePostDataUpdates(person)

      queries.runAll.unsafeRunSync()

      findPersons shouldBe Set(toResultRow(person))
    }

    "return queries removing person with the same email but no gitLabId " +
      "and relinking edges to the removed person to the person having gitLabId " +
      "- case when person without GitLabId is passed in" in {
        val email            = userEmails.generateOne
        val personNoGitLab   = personEntities(withoutGitLabId, fixed(email.some)).generateOne.to[entities.Person]
        val personWithGitLab = personEntities(withGitLabId, fixed(email.some)).generateOne.to[entities.Person]

        loadToStore(personNoGitLab, personWithGitLab)

        val entityId = EntityId.of(httpUrls().generateOne)
        val property = properties.generateOne
        loadToStore(
          JsonLD.entity(entityId,
                        entityTypesObject.generateOne,
                        Map(property -> personNoGitLab.resourceId.asEntityId.asJsonLD)
          )
        )

        findPersons                     shouldBe Set(toResultRow(personNoGitLab), toResultRow(personWithGitLab))
        findObjects(entityId, property) shouldBe Set(personNoGitLab.resourceId.asEntityId.show)

        val queries = preparePostDataUpdates(personNoGitLab)

        queries.runAll.unsafeRunSync()

        findPersons                     shouldBe Set(toResultRow(personWithGitLab))
        findObjects(entityId, property) shouldBe Set(personWithGitLab.resourceId.asEntityId.show)
      }

    "return queries removing person with the same email but no gitLabId " +
      "and relinking edges to the removed person to the person having gitLabId " +
      "- case when person with GitLabId is passed in" in {
        val email            = userEmails.generateOne
        val personNoGitLab   = personEntities(withoutGitLabId, fixed(email.some)).generateOne.to[entities.Person]
        val personWithGitLab = personEntities(withGitLabId, fixed(email.some)).generateOne.to[entities.Person]

        loadToStore(personNoGitLab, personWithGitLab)

        val entityId = EntityId.of(httpUrls().generateOne)
        val property = properties.generateOne
        loadToStore(
          JsonLD.entity(entityId,
                        entityTypesObject.generateOne,
                        Map(property -> personNoGitLab.resourceId.asEntityId.asJsonLD)
          )
        )

        findPersons                     shouldBe Set(toResultRow(personNoGitLab), toResultRow(personWithGitLab))
        findObjects(entityId, property) shouldBe Set(personNoGitLab.resourceId.asEntityId.show)

        val queries = preparePostDataUpdates(personWithGitLab)

        queries.runAll.unsafeRunSync()

        findPersons                     shouldBe Set(toResultRow(personWithGitLab))
        findObjects(entityId, property) shouldBe Set(personWithGitLab.resourceId.asEntityId.show)
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

  private def findObjects(entityId: EntityId, property: Property): Set[String] =
    runQuery(s"""|SELECT ?object
                 |WHERE { OPTIONAL { <${entityId.show}> <${property.show}> ?object } }
                 |""".stripMargin)
      .unsafeRunSync()
      .map(_.get("object"))
      .toSet
      .flatten

  private lazy val toResultRow
      : entities.Person => (String, Option[String], Option[String], Option[String], Option[Int]) = {
    case entities.Person(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId) =>
      (resourceId.value,
       Some(name.value),
       maybeEmail.map(_.value),
       maybeAffiliation.map(_.value),
       maybeGitLabId.map(_.value)
      )
  }
}
