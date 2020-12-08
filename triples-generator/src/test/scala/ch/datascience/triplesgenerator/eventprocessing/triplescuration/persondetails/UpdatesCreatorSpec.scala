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
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.ResourceId
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.Project
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsGenerators.persons
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import ch.datascience.rdfstore.entities.ProjectsGenerators._

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "prepareUpdates" should {

    "generate query which upserts all when the GitLabId is matched and the resourceId is in JENA" +
      "but the gitLabId is *NOT* in JENA" in new TestCase {
        val gitLabId   = userGitLabIds.generateOne
        val person     = persons(None).generateOne
        val resourceId = person.toResourceId

        val newName  = userNames.generateOne
        val newEmail = userEmails.generateOption

        loadToStore(person.toJsonLd)

        val updatesGroup = updatesCreator.prepareUpdates[IO](
          person.copy(maybeGitLabId = gitLabId.some, name = newName, maybeEmail = newEmail)
        )

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

    "generate query which upserts everything when the resourceId for the GitLab Id is different" in new TestCase { // This could happen if a user changes their email
      val gitLabId = userGitLabIds.generateOne
      val person   = persons(gitLabId.some).generateOne
      val project1 = projects.generateOne.copy(maybeCreator = Some(person.toEntitiesPerson))
      val project2 = projects.generateOne.copy(maybeCreator = Some(person.toEntitiesPerson))
      loadToStore(person.toJsonLd, project1.asJsonLD, project2.asJsonLD)

      findProjectsCreatorIds shouldBe Set(person.toResourceId.value)

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

      findProjectsCreatorIds shouldBe Set(updatedPerson.toResourceId.value)
    }

//    "generate query creating person with a name and email" in new TestCase {
//      forAll { (name: Name, email: Email) =>
//        findPersons shouldBe empty
//
//        val personId = Person(name, email).asJsonLD.entityId.get
//
//        val updatesGroup = updatesCreator.prepareUpdates[IO](
//          UpdatedPerson(ResourceId(personId), name, Some(email))
//        )
//
//        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()
//
//        val persons = findPersons
//        persons should contain theSameElementsAs Set(
//          (personId.value, Some(name.value), Some(email.value), None)
//        )
//
//        clearDataset()
//      }
//    }
//
//    "generate query creating a Person with a name only" in new TestCase {
//      forAll { name: Name =>
//        val personId = Person(name).asJsonLD.entityId.get
//
//        val updatesGroup = updatesCreator.prepareUpdates[IO](
//          UpdatedPerson(ResourceId(personId), name, maybeEmail = None)
//        )
//
//        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()
//
//        val actual = findPersons
//        actual should have size 1
//        val (_, Some(actualName), None, None) = actual.head
//        actualName shouldBe name.value
//        clearDataset()
//      }
//    }
//
//    "generate query updating person's name and removing label - case when person id is known" in new TestCase {
//      forAll { (name1: Name, email1: Email, name2: Name, email2: Email) =>
//        val person1Json = Person(name1, email1).asJsonLD
//        val person1Id   = person1Json.entityId.get
//        val person2Json = Person(name2, email2).asJsonLD
//        val person2Id   = person2Json.entityId.get
//
//        loadToStore(person1Json, person2Json)
//
//        findPersons should contain theSameElementsAs Set(
//          (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
//          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
//        )
//
//        val name1Updated = userNames.generateOne
//
//        val updatesGroup = updatesCreator.prepareUpdates[IO](
//          UpdatedPerson(ResourceId(person1Id), name1Updated, Some(email1))
//        )
//
//        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()
//
//        findPersons should contain theSameElementsAs Set(
//          (person1Id.value, Some(name1Updated.value), Some(email1.value), None),
//          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
//        )
//
//        clearDataset()
//      }
  }

//    "generate query updating person's name and removing email and label - case when person id is known" in new TestCase {
//      forAll { (name1: Name, email1: Email, name2: Name, email2: Email) =>
//        val person1Json = Person(name1, email1).asJsonLD
//        val person1Id   = person1Json.entityId.get
//        val person2Json = Person(name2, email2).asJsonLD
//        val person2Id   = person2Json.entityId.get
//
//        loadToStore(person1Json, person2Json)
//
//        findPersons should contain theSameElementsAs Set(
//          (person1Id.value, Some(name1.value), Some(email1.value), Some(name1.value)),
//          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
//        )
//
//        val name1Updated = userNames.generateOne
//
//        val updatesGroup = updatesCreator.prepareUpdates[IO](
//          UpdatedPerson(ResourceId(person1Id), name1Updated, maybeEmail = None)
//        )
//
//        updatesGroup.generateUpdates().foldF(throw _, _.runAll).unsafeRunSync()
//
//        findPersons should contain theSameElementsAs Set(
//          (person1Id.value, Some(name1Updated.value), None, None),
//          (person2Id.value, Some(name2.value), Some(email2.value), Some(name2.value))
//        )
//
//        clearDataset()
//      }
//    }
//
//  }

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
                 |  ?projectId rdf:type schema:Project ;
                 |  schema:creator ?id
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

  }

//  private implicit class ProjectOps(project: Project) {
//    import ch.datascience.rdfstore.entities
//
//    lazy val toJsonLd: JsonLD = entities.Project(project.path, project.name, project.dateCreated, project.maybeCreator, project.)
//  }

  private trait TestCase {
    val updatesCreator = new UpdatesCreator(gitLabApiUrl)
  }
}
