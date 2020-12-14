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
package persondetails

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.ResourceId
import ch.datascience.rdfstore.entities.EntitiesGenerators._
import ch.datascience.rdfstore.{InMemoryRdfStore, JsonLDTriples}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsGenerators.persons
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "prepareUpdates" should {

    "generate query which upserts all when the GitLabId is matched and the resourceId is in JENA" +
      "but the gitLabId is *NOT* in JENA" in new TestCase {
        val gitLabId   = userGitLabIds.generateOne
        val person     = persons(None).generateOne
        val resourceId = person.toResourceId

        loadToStore(person.toJsonLd)

        findPersons shouldBe Set(
          (resourceId.value, Some(person.name.value), person.maybeEmail.map(_.value), None, None)
        )

        val newName  = userNames.generateOne
        val newEmail = userEmails.generateOption

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.copy(maybeGitLabId = gitLabId.some, name = newName, maybeEmail = newEmail)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons shouldBe Set(
          (resourceId.value,
           Some(newName.value),
           newEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which upserts all when the GitLabId is matched and the resourceId is in JENA" +
      "but the gitLabId *IS* in JENA" in new TestCase {
        val gitLabId   = userGitLabIds.generateOne
        val person     = persons(gitLabId.some).generateOne
        val resourceId = person.toResourceId

        val newName  = userNames.generateOne
        val newEmail = userEmails.generateOption

        loadToStore(person.toJsonLd)

        val updatesGroup = updatesCreator.prepareUpdates[IO](person.copy(name = newName, maybeEmail = newEmail))

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (resourceId.value,
           Some(newName.value),
           newEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which deletes the existing GitLabId and upserts everything when the GitLabId is matched " +
      "and the existing GitLabId in JENA is different" in new TestCase { // This could happen if a user deletes their account and creates a new one
        val existingGitLabId = userGitLabIds.generateOne
        val person           = persons(existingGitLabId.some).generateOne
        val resourceId       = person.toResourceId

        val newGitLabId = userGitLabIds.generateOne

        loadToStore(person.toJsonLd)

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.copy(maybeGitLabId = newGitLabId.some)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (resourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / newGitLabId).map(_.toString),
           Some(newGitLabId.value)
          )
        )
      }

    "generate query which upserts everything when the GitLabId is matched " +
      "and nothing in JENA " in new TestCase {
        val gitLabId   = userGitLabIds.generateOne
        val person     = persons(gitLabId.some).generateOne
        val resourceId = person.toResourceId

        val updatesGroup = updatesCreator.prepareUpdates[IO](person)

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (resourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which upserts everything when the resourceId for the GitLab Id is different" in new TestCase {
      // This could happen if a user changes their email
      val gitLabId = userGitLabIds.generateOne
      val person   = persons(gitLabId.some).generateOne
      val project1 = projectEntities.generateOne.copy(maybeCreator = Some(person.toEntitiesPerson))
      val project2 = projectEntities.generateOne.copy(maybeCreator = Some(person.toEntitiesPerson))
      val activity = activityEntities.generateOne.copy(committer = person.toEntitiesPerson, project = project1)

      loadToStore(person.toJsonLd, project1.asJsonLD, project2.asJsonLD, activity.asJsonLD)

      findProjectsCreatorIds     shouldBe Set(person.toResourceId.value)
      findActivitiesCommitterIds shouldBe Set(person.toResourceId.value)

      val updatedPerson = persons(gitLabId.some).generateOne

      val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson)

      updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

      findPersons should contain theSameElementsAs Set(
        (updatedPerson.toResourceId.value,
         Some(updatedPerson.name.value),
         updatedPerson.maybeEmail.map(_.value),
         Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
         Some(gitLabId.value)
        )
      )

      findProjectsCreatorIds     shouldBe Set(updatedPerson.toResourceId.value)
      findActivitiesCommitterIds shouldBe Set(updatedPerson.toResourceId.value)
    }

    "generate query which upserts name or email and remove GitLab Id " +
      "when the GitLab Id matching was not successful but there's GitLab Id for that person in JENA" in new TestCase {
        // This could happen if
        // * a user lost his GitLab account
        // * a user changed either email or name so the resourceId stays unchanged

        val gitLabId = userGitLabIds.generateOne
        val email    = userEmails.generateOne
        val person   = persons(gitLabId.some).generateOne.copy(maybeEmail = Some(email)).regenerateResourceId
        loadToStore(person.toJsonLd)

        findPersons shouldBe Set(
          (person.toResourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )

        val updatedPerson = person.copy(name = userNames.generateOne, maybeGitLabId = None)

        val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson)

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (updatedPerson.id.value, Some(updatedPerson.name.value), Some(email.value), None, None)
        )
      }

    "generate query which upserts name if a person has an email " +
      "when the GitLab Id matching was not successful and there's no GitLab Id for that person in JENA" in new TestCase {

        val email  = userEmails.generateOne
        val person = persons(maybeGitLabId = None).generateOne.copy(maybeEmail = Some(email)).regenerateResourceId
        loadToStore(person.toJsonLd)

        findPersons shouldBe Set(
          (person.toResourceId.value, Some(person.name.value), person.maybeEmail.map(_.value), None, None)
        )

        val updatedPerson = person.copy(name = userNames.generateOne)

        val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson)

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (updatedPerson.id.value, Some(updatedPerson.name.value), Some(email.value), None, None)
        )
      }

    "generate query which upserts both email and name " +
      "when the GitLab Id matching was not successful and this person exists in JENA but without name and email" in new TestCase {

        val person = persons(maybeGitLabId = None).generateOne
        loadToStore(
          JsonLDTriples(person.toJsonLd.toJson.remove(schema / "name").remove(schema / "email"))
        )

        findPersons shouldBe Set(
          (person.toResourceId.value, None, None, None, None)
        )

        val updatedPerson = person.copy(name = userNames.generateOne, maybeEmail = userEmails.generateOption)

        val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson)

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (updatedPerson.id.value, Some(updatedPerson.name.value), updatedPerson.maybeEmail.map(_.value), None, None)
        )
      }
  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String], Option[Int])] =
    runQuery(s"""|SELECT ?id ?name ?email ?sameAsId ?gitlabId
                 |WHERE {
                 |  ?id rdf:type schema:Person .
                 |  OPTIONAL { ?id schema:name ?name } .
                 |  OPTIONAL { ?id schema:email ?email } .
                 |  OPTIONAL { ?id schema:sameAs ?sameAsId } .
                 |  OPTIONAL { ?sameAsId schema:identifier ?gitlabId; 
                 |                       rdf:type schema:URL;
                 |                       schema:additionalType 'GitLab'
                 |  } .
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row.get("name"), row.get("email"), row.get("sameAsId"), row.get("gitlabId").map(_.toInt)))
      .toSet

  private def findProjectsCreatorIds: Set[String] =
    runQuery(s"""|SELECT ?id
                 |WHERE {
                 |  ?projectId rdf:type schema:Project;
                 |             schema:creator ?id
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("id"))
      .toSet

  private def findActivitiesCommitterIds: Set[String] =
    runQuery(s"""|SELECT ?id
                 |WHERE {
                 |  ?projectId rdf:type prov:Activity;
                 |             prov:wasAssociatedWith ?id.
                 |  ?id rdf:type schema:Person.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("id"))
      .toSet

  private implicit lazy val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  private implicit lazy val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

  private implicit class PersonOps(person: Person) {
    import ch.datascience.rdfstore.entities

    lazy val toResourceId: ResourceId =
      users.ResourceId(
        entities
          .Person(person.name, person.maybeEmail)
          .asJsonLD
          .entityId
          .getOrElse(throw new Exception("Person resourceId cannot be found"))
      )

    lazy val toJsonLd: JsonLD = toEntitiesPerson.asJsonLD

    lazy val toEntitiesPerson = entities.Person(person.name, person.maybeEmail, maybeGitLabId = person.maybeGitLabId)

    lazy val regenerateResourceId: Person = person.copy(id = person.toResourceId)
  }

  private trait TestCase {
    val updatesCreator = new UpdatesCreator(gitLabApiUrl)
  }
}
