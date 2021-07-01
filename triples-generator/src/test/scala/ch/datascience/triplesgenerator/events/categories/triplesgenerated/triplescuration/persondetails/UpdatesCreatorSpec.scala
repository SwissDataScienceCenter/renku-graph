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

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.testentities._
import ch.datascience.rdfstore.InMemoryRdfStore
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "prepareUpdates" should {

    "generate query which upserts all when the GitLabId is matched and the resourceId is in JENA" +
      "but the gitLabId is *NOT* in JENA" in {

        val person = personEntities(withoutGitLabId).generateOne

        loadToStore(person)

        findPersons shouldBe Set(
          (person.resourceId.value, Some(person.name.value), person.maybeEmail.map(_.value), None, None)
        )

        val gitLabId = userGitLabIds.generateOne
        val newName  = userNames.generateOne
        val newEmail = userEmails.generateOption

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.to[persondetails.Person].copy(maybeGitLabId = gitLabId.some, name = newName, maybeEmail = newEmail)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons shouldBe Set(
          (person.resourceId.value,
           Some(newName.value),
           newEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which upserts all when the GitLabId is matched and the resourceId is in JENA" +
      "and the gitLabId *IS* in JENA" in {
        val gitLabId = userGitLabIds.generateOne
        val person   = personEntities(fixed(gitLabId.some)).generateOne

        loadToStore(person)

        val newName  = userNames.generateOne
        val newEmail = userEmails.generateOption

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.to[persondetails.Person].copy(name = newName, maybeEmail = newEmail)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person.resourceId.value,
           Some(newName.value),
           newEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which deletes the existing GitLabId and upserts everything when the GitLabId is matched " +
      "and the existing GitLabId in JENA is different" in { // This could happen if a user deletes their account and creates a new one
        val existingGitLabId = userGitLabIds.generateOne
        val person           = personEntities(fixed(existingGitLabId.some)).generateOne

        loadToStore(person)

        val newGitLabId = userGitLabIds.generateOne

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.to[persondetails.Person].copy(maybeGitLabId = newGitLabId.some)
        )

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person.resourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / newGitLabId).map(_.toString),
           Some(newGitLabId.value)
          )
        )
      }

    "generate query which inserts everything when the GitLabId is matched " +
      "and nothing in JENA " in {
        val gitLabId = userGitLabIds.generateOne
        val person   = personEntities(fixed(gitLabId.some)).generateOne

        val updatesGroup = updatesCreator.prepareUpdates[IO](person.to[persondetails.Person])

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (person.resourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )
      }

    "generate query which upserts everything when the resourceId for the GitLab id is different" in {
      // This could happen if a user changes their email
      val gitLabId = userGitLabIds.generateOne
      val person   = personEntities(fixed(gitLabId.some)).generateOne
      val project1 = projectEntities[Project.ForksCount.Zero](visibilityAny).generateOne
        .copy(maybeCreator = Some(person), members = Set(person))
      val project2 = projectEntities[Project.ForksCount.Zero](visibilityAny).generateOne
        .copy(maybeCreator = Some(person), members = Set.empty)
      val dataset = {
        val orig = datasetEntities(datasetProvenanceInternal, fixed(project1)).generateOne
        orig.copy(provenance = orig.provenance.copy(creators = Set(person)))
      }

      loadToStore(person.asJsonLD, project1.asJsonLD, project2.asJsonLD, dataset.asJsonLD)

      findProjectsCreatorIds shouldBe Set(person.resourceId.value)
      findDatasetCreatorsIds shouldBe Set(person.resourceId.value)

      val updatedPerson = personEntities(fixed(gitLabId.some)).generateOne

      val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson.to[persondetails.Person])

      updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

      findPersons should contain theSameElementsAs Set(
        (updatedPerson.resourceId.value,
         Some(updatedPerson.name.value),
         updatedPerson.maybeEmail.map(_.value),
         Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
         Some(gitLabId.value)
        )
      )

      findProjectsCreatorIds shouldBe Set(updatedPerson.resourceId.value)
      findDatasetCreatorsIds shouldBe Set(updatedPerson.resourceId.value)
    }

    "generate query which upserts name or email and remove GitLab Id " +
      "when the GitLab Id matching was not successful but there's GitLab Id for that person in JENA" in {
        // This could happen if
        // * a user lost his GitLab account
        // * a user changed either email or name so the resourceId stays unchanged

        val gitLabId = userGitLabIds.generateOne
        val email    = userEmails.generateOne
        val person   = personEntities(fixed(gitLabId.some), fixed(email.some)).generateOne

        loadToStore(person)

        findPersons shouldBe Set(
          (person.resourceId.value,
           Some(person.name.value),
           person.maybeEmail.map(_.value),
           Some(gitLabApiUrl / "users" / gitLabId).map(_.toString),
           Some(gitLabId.value)
          )
        )

        val updatedPerson = person.copy(name = userNames.generateOne, maybeGitLabId = None)

        val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson.to[persondetails.Person])

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (updatedPerson.resourceId.value, Some(updatedPerson.name.value), Some(email.value), None, None)
        )
      }

    "generate query which upserts name if a person has an email " +
      "when the GitLab Id matching was not successful and there's no GitLab Id for that person in JENA" in {

        val email  = userEmails.generateOne
        val person = personEntities(withoutGitLabId, fixed(email.some)).generateOne

        loadToStore(person)

        findPersons shouldBe Set(
          (person.resourceId.value, Some(person.name.value), person.maybeEmail.map(_.value), None, None)
        )

        val updatedPerson = person.copy(name = userNames.generateOne)

        val updatesGroup = updatesCreator.prepareUpdates[IO](updatedPerson.to[persondetails.Person])

        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()

        findPersons should contain theSameElementsAs Set(
          (updatedPerson.resourceId.value, Some(updatedPerson.name.value), Some(email.value), None, None)
        )
      }
  }

  private def findPersons: Set[(String, Option[String], Option[String], Option[String], Option[Int])] =
    runQuery(s"""|SELECT ?id ?name ?email ?sameAsId ?gitlabId
                 |WHERE {
                 |  ?id a schema:Person .
                 |  OPTIONAL { ?id schema:name ?name } .
                 |  OPTIONAL { ?id schema:email ?email } .
                 |  OPTIONAL { ?id schema:sameAs ?sameAsId } .
                 |  OPTIONAL { ?sameAsId schema:identifier ?gitlabId; 
                 |                       a schema:URL;
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
                 |  ?projectId a schema:Project;
                 |             schema:creator ?id
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("id"))
      .toSet

  private def findDatasetCreatorsIds: Set[String] =
    runQuery(s"""|SELECT ?id
                 |WHERE {
                 |  ?projectId a schema:Dataset;
                 |             schema:creator ?id.
                 |  ?id a schema:Person.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("id"))
      .toSet

  private lazy val updatesCreator = new UpdatesCreator(gitLabApiUrl)
}
