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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import java.time.Instant

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators
import ch.datascience.generators.CommonGraphGenerators.emails
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectCreatedDates, projectNames, projectPaths}
import ch.datascience.graph.model.projects.{DateCreated, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.entities.{Person, Project}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "deleteWasDerivedFrom" should {

    "generate query deleting 'prov:wasDerivedFrom' triple from a given project" in new TestCase {
      val maybeParent @ Some(parent) = projects().generateSome
      val child1                     = projects(maybeParentProject = maybeParent).generateOne
      val child2                     = projects(maybeParentProject = maybeParent).generateOne

      loadToStore(child1.asJsonLD, child2.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> Some(parent.resourceId.value),
        child2.resourceId.value -> Some(parent.resourceId.value),
        parent.resourceId.value -> None
      )

      updatesCreator.deleteWasDerivedFrom(child1.resourceId).run

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> None,
        child2.resourceId.value -> Some(parent.resourceId.value),
        parent.resourceId.value -> None
      )
    }
  }

  "insertWasDerivedFrom" should {

    "generate query inserting 'prov:wasDerivedFrom' triple to a given project" in new TestCase {
      val maybeParent @ Some(parent) = projects().generateSome
      val child1                     = projects(maybeParentProject = None).generateOne
      val child2                     = projects(maybeParentProject = None).generateOne

      loadToStore(child1.asJsonLD, child2.asJsonLD, parent.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> None,
        child2.resourceId.value -> None,
        parent.resourceId.value -> None
      )

      updatesCreator.insertWasDerivedFrom(child1.resourceId, parent.path).run

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> Some(parent.resourceId.value),
        child2.resourceId.value -> None,
        parent.resourceId.value -> None
      )
    }
  }

  "recreateWasDerivedFrom" should {

    "generate queries deleting and inserting 'prov:wasDerivedFrom' triple to a given project" in new TestCase {
      val maybeParent1 @ Some(parent1) = projects().generateSome
      val maybeParent2 @ Some(parent2) = projects().generateSome
      val child1                       = projects(maybeParentProject = maybeParent1).generateOne
      val child2                       = projects(maybeParentProject = maybeParent2).generateOne

      loadToStore(child1.asJsonLD, child2.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value  -> Some(parent1.resourceId.value),
        child2.resourceId.value  -> Some(parent2.resourceId.value),
        parent1.resourceId.value -> None,
        parent2.resourceId.value -> None
      )

      updatesCreator.recreateWasDerivedFrom(child1.resourceId, parent2.path).run

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value  -> Some(parent2.resourceId.value),
        child2.resourceId.value  -> Some(parent2.resourceId.value),
        parent1.resourceId.value -> None,
        parent2.resourceId.value -> None
      )
    }
  }

  "swapCreator" should {

    "change change Project's link to a Person to the given one" in new TestCase {
      val creator1 = creators(emails.generateSome).generateOne
      val creator2 = creators(Some(Email("y9Cr+ygoi83@zwpm"))).generateOne
      val project1 = projects(creator1).generateOne
      val project2 = projects(creator2).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value)),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value))
      )

      updatesCreator.swapCreator(project1.resourceId, creator2.resourceId).run

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value)),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value))
      )
    }
  }

  "addNewCreator" should {

    "create a new Person and link it to the given Project" in new TestCase {
      val creator1 = creators().generateOne
      val creator2 = creators().generateOne
      val project1 = projects(creator1).generateOne
      val project2 = projects(creator2).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value)),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value))
      )

      val newCreator = creators().generateOne

      updatesCreator.addNewCreator(project1.resourceId, newCreator.maybeEmail, newCreator.name.some).run

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, newCreator.name.value, newCreator.maybeEmail.map(_.value)),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value))
      )
    }
  }

  "recreateDateCreated" should {

    "create a new Person and link it to the given Project" in new TestCase {
      val project1 = projects().generateOne
      val project2 = projects().generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, project1.dateCreated),
        (project2.resourceId.value, project2.dateCreated)
      )

      val newDateCreated = projectCreatedDates.generateOne

      updatesCreator.recreateDateCreated(project1.resourceId, newDateCreated).run

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, newDateCreated),
        (project2.resourceId.value, project2.dateCreated)
      )
    }
  }

  private trait TestCase {
    val updatesCreator = new UpdatesCreator(renkuBaseUrl)
  }

  private implicit class ProjectOps(project: Project) {
    lazy val resourceId: ResourceId = project.asJsonLD.entityId
      .map(id => ResourceId(id.value))
      .getOrElse(fail("projects.ResourceId cannot be obtained"))
  }

  private implicit class PersonOps(person: Person) {
    lazy val resourceId: users.ResourceId = person.asJsonLD.entityId
      .map(id => users.ResourceId(id.value))
      .getOrElse(fail("users.ResourceId cannot be obtained"))
  }

  private implicit class UpdatesRunner(updates: List[CuratedTriples.Update]) {
    lazy val run = (updates.map(_.query) map runUpdate).sequence.unsafeRunSync()
  }

  private def creators(maybeEmail: Option[Email] = emails.generateOption): Gen[Person] =
    for {
      name <- CommonGraphGenerators.names
    } yield Person(name, maybeEmail)

  private def projects(creator:            Person          = creators().generateOne,
                       maybeParentProject: Option[Project] = None): Gen[Project] =
    for {
      path        <- projectPaths
      name        <- projectNames
      createdDate <- projectCreatedDates
    } yield Project(path, name, createdDate, creator, maybeParentProject)

  private def findDerivedFrom: Set[(String, Option[String])] =
    runQuery(s"""|SELECT ?id ?maybeParentId
                 |WHERE {
                 |  ?id rdf:type schema:Project .
                 |  OPTIONAL { ?id prov:wasDerivedFrom ?maybeParentId }.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row.get("maybeParentId")))
      .toSet

  private def findCreators: Set[(String, String, Option[String])] =
    runQuery(s"""|SELECT ?id ?name ?email 
                 |WHERE {
                 |  ?id rdf:type schema:Project;
                 |      schema:creator ?creatorId.
                 |  ?creatorId schema:name ?name. 
                 |  OPTIONAL { ?creatorId schema:email ?email } 
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row("name"), row.get("email")))
      .toSet

  private def findDateCreated: Set[(String, DateCreated)] =
    runQuery(s"""|SELECT ?id ?dateCreated 
                 |WHERE {
                 |  ?id rdf:type schema:Project;
                 |      schema:dateCreated ?dateCreated.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), DateCreated(Instant.parse(row("dateCreated")))))
      .toSet
}
