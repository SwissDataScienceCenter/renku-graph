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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.{projectCreatedDates, projectNames, projectVisibilities}
import ch.datascience.graph.model.projects.{DateCreated, Name, ResourceId, Visibility}
import ch.datascience.graph.model.{projects, users}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.entities.EntitiesGenerators._
import ch.datascience.rdfstore.entities.{Person, Project}
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQuery}
import eu.timepit.refined.auto._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class UpdatesQueryCreatorSpec extends AnyWordSpec with InMemoryRdfStore with Matchers {

  "updateWasDerivedFrom" should {

    "generate query deleting 'prov:wasDerivedFrom' triple from a given project when there is no fork" in new TestCase {
      val (parent, children) = projectEntities[Project.ForksCount.Zero]().generateOne.fork(times = 2)
      val child1             = children.head
      val child2             = children.tail.head

      loadToStore(child1.asJsonLD, child2.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> Some(parent.resourceId.value),
        child2.resourceId.value -> Some(parent.resourceId.value),
        parent.resourceId.value -> None
      )

      updatesQueryCreator.updateWasDerivedFrom(child1.path, None).runAll.unsafeRunSync()

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> None,
        child2.resourceId.value -> Some(parent.resourceId.value),
        parent.resourceId.value -> None
      )
    }

    "generate query inserting 'prov:wasDerivedFrom' triple to a given project when there is no derivedFrom" in new TestCase {
      val Some(parent) = entitiesProjects().generateSome
      val child1       = entitiesProjects(maybeParentProject = None).generateOne
      val child2       = entitiesProjects(maybeParentProject = None).generateOne

      loadToStore(child1.asJsonLD, child2.asJsonLD, parent.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> None,
        child2.resourceId.value -> None,
        parent.resourceId.value -> None
      )

      updatesQueryCreator.updateWasDerivedFrom(child1.path, parent.path.some).runAll.unsafeRunSync()

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value -> Some(parent.resourceId.value),
        child2.resourceId.value -> None,
        parent.resourceId.value -> None
      )
    }

    "generate a query updating 'prov:wasDerivedFrom' triple to a given project when there is already a derivedFrom" in new TestCase {
      val maybeParent1 @ Some(parent1) = entitiesProjects().generateSome
      val maybeParent2 @ Some(parent2) = entitiesProjects().generateSome
      val child1                       = entitiesProjects(maybeParentProject = maybeParent1).generateOne
      val child2                       = entitiesProjects(maybeParentProject = maybeParent2).generateOne

      loadToStore(child1.asJsonLD, child2.asJsonLD)

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value  -> Some(parent1.resourceId.value),
        child2.resourceId.value  -> Some(parent2.resourceId.value),
        parent1.resourceId.value -> None,
        parent2.resourceId.value -> None
      )

      updatesQueryCreator.updateWasDerivedFrom(child1.path, parent2.path.some).runAll.unsafeRunSync()

      findDerivedFrom should contain theSameElementsAs Set(
        child1.resourceId.value  -> Some(parent2.resourceId.value),
        child2.resourceId.value  -> Some(parent2.resourceId.value),
        parent1.resourceId.value -> None,
        parent2.resourceId.value -> None
      )
    }
  }

  "swapCreator" should {

    "replace the current Project's creator with the given one" in new TestCase {
      val creator1 = persons.generateOne
      val creator2 = persons.generateOne
      val project1 = entitiesProjects(maybeCreator = creator1.some).generateOne
      val project2 = entitiesProjects(maybeCreator = creator2.some).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )

      updatesQueryCreator.swapCreator(project1.path, creator2.resourceId).runAll.unsafeRunSync()

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )
    }

    "add a new creator to the Project when there is no creator linked to the project" in new TestCase {
      val creator1 = persons.generateOne
      val project1 = entitiesProjects(maybeCreator = None).generateOne
      val project2 = entitiesProjects(maybeCreator = creator1.some).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project2.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None)
      )

      updatesQueryCreator.swapCreator(project1.path, creator1.resourceId).runAll.unsafeRunSync()

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None),
        (project2.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None)
      )
    }
  }

  "unlinkCreator" should {

    "remove creator from the project" in new TestCase {
      val creator1 = persons.generateOne
      val creator2 = persons.generateOne
      val project1 = entitiesProjects(maybeCreator = creator1.some).generateOne
      val project2 = entitiesProjects(maybeCreator = creator2.some).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )

      updatesQueryCreator.unlinkCreator(project1.path).runAll.unsafeRunSync()

      findCreators should contain theSameElementsAs Set(
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )
    }
  }

  "addNewCreator" should {

    "create a new Person and link it to the given Project replacing the old creator on the project" in new TestCase {
      val creator1 = persons.generateOne
      val creator2 = persons.generateOne
      val project1 = entitiesProjects(maybeCreator = creator1.some).generateOne
      val project2 = entitiesProjects(maybeCreator = creator2.some).generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, creator1.name.value, creator1.maybeEmail.map(_.value), None),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )

      val newCreator = gitLabCreator().generateOne

      updatesQueryCreator
        .addNewCreator(project1.path, newCreator)
        .runAll
        .unsafeRunSync()

      findCreators should contain theSameElementsAs Set(
        (project1.resourceId.value, newCreator.name.value, None, newCreator.gitLabId.value.some),
        (project2.resourceId.value, creator2.name.value, creator2.maybeEmail.map(_.value), None)
      )
    }
  }

  "recreateDateCreated" should {

    "update the Project's createdDate - case when there is already a dateCreated" in new TestCase {
      val project1 = entitiesProjects().generateOne
      val project2 = entitiesProjects().generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, project1.dateCreated.some),
        (project2.resourceId.value, project2.dateCreated.some)
      )

      val newDateCreated = projectCreatedDates.generateOne

      updatesQueryCreator.updateDateCreated(project1.path, newDateCreated).runAll.unsafeRunSync()

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, newDateCreated.some),
        (project2.resourceId.value, project2.dateCreated.some)
      )
    }

    "update the Project's createdDate - case when there is no dateCreated" in new TestCase {
      val project1 = entitiesProjects().generateOne
      val project2 = entitiesProjects().generateOne

      loadToStore(project1.asJsonLD, project2.asJsonLD)

      removeDateCreated(project1.resourceId)

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, None),
        (project2.resourceId.value, project2.dateCreated.some)
      )

      val newDateCreated = projectCreatedDates.generateOne

      updatesQueryCreator.updateDateCreated(project1.path, newDateCreated).runAll.unsafeRunSync()

      findDateCreated should contain theSameElementsAs Set(
        (project1.resourceId.value, newDateCreated.some),
        (project2.resourceId.value, project2.dateCreated.some)
      )
    }
  }
  "upsertVisibility" should {
    "update the Project's visibility - case when visibility exists on project already" in new TestCase {
      val initialVisibility: projects.Visibility = projectVisibilities.generateOne
      val project = entitiesProjects(maybeVisibility = Some(initialVisibility)).generateOne
      loadToStore(project.asJsonLD)

      val newVisibility = projectVisibilities.generateDifferentThan(initialVisibility)

      findVisibility(project.resourceId) shouldBe Some(initialVisibility)

      updatesQueryCreator.upsertVisibility(project.path, newVisibility).runAll.unsafeRunSync()

      findVisibility(project.resourceId) shouldBe Some(newVisibility)

    }

    "insert the Project's visibility - case when it didn't previously exist" in new TestCase {

      val project = entitiesProjects(maybeVisibility = None).generateOne
      loadToStore(project.asJsonLD)

      val newVisibility = projectVisibilities.generateOne

      findVisibility(project.resourceId) shouldBe None

      updatesQueryCreator.upsertVisibility(project.path, newVisibility).runAll.unsafeRunSync()

      findVisibility(project.resourceId) shouldBe Some(newVisibility)

    }

  }

  "upsertName" should {
    "update the Project's name" in new TestCase {
      val initialName: projects.Name = projectNames.generateOne
      val project = entitiesProjects(name = initialName).generateOne
      loadToStore(project.asJsonLD)

      val newName = projectNames.generateDifferentThan(initialName)

      findName(project.resourceId) shouldBe initialName

      updatesQueryCreator.upsertName(project.path, newName).runAll.unsafeRunSync()

      findName(project.resourceId) shouldBe newName

    }
  }

  private trait TestCase {
    val updatesQueryCreator = new UpdatesQueryCreator(renkuBaseUrl, gitLabApiUrl)
  }

  private implicit class ProjectOps(project: Project) {
    lazy val resourceId: ResourceId = project.asJsonLD.entityId
      .map(id => ResourceId(id))
      .getOrElse(fail("projects.ResourceId cannot be obtained"))
  }

  private implicit class PersonOps(person: Person) {
    lazy val resourceId: users.ResourceId = person.asJsonLD.entityId
      .map(id => users.ResourceId(id))
      .getOrElse(fail("users.ResourceId cannot be obtained"))
  }

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

  private def findCreators: Set[(String, String, Option[String], Option[Int])] =
    runQuery(s"""|SELECT ?id ?name ?email ?creatorId ?gitLabId
                 |WHERE {
                 |  ?id rdf:type schema:Project;
                 |      schema:creator ?creatorId.
                 |  ?creatorId schema:name ?name.
                 |  OPTIONAL { ?creatorId schema:email ?email }
                 |  OPTIONAL { 
                 |    ?creatorId schema:sameAs ?sameAsId.
                 |    ?sameAsId rdf:type schema:URL;
                 |              schema:additionalType 'GitLab';
                 |              schema:identifier ?gitLabId;
                 |  }
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row("name"), row.get("email"), row.get("gitLabId").flatMap(_.toIntOption)))
      .toSet

  private def findDateCreated: Set[(String, Option[DateCreated])] =
    runQuery(s"""|SELECT ?id ?dateCreated
                 |WHERE {
                 |  ?id rdf:type schema:Project
                 |  OPTIONAL { ?id  schema:dateCreated ?dateCreated }
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("id"), row.get("dateCreated").map(dateCreated => DateCreated(Instant.parse(dateCreated)))))
      .toSet

  private def removeDateCreated(projectId: ResourceId): Unit =
    runUpdate(
      SparqlQuery(
        name = "delete date created",
        prefixes = Set("PREFIX schema: <http://schema.org/>"),
        s"""| DELETE { ${projectId.showAs[RdfResource]} schema:dateCreated ?dateCreated }
            | WHERE { ${projectId.showAs[RdfResource]} schema:dateCreated ?dateCreated }
            |""".stripMargin
      )
    ).unsafeRunSync()

  private def findVisibility(projectId: ResourceId): Option[Visibility] =
    runQuery(
      s"""|SELECT ?visibility
          |WHERE {
          |  OPTIONAL {  ${projectId.showAs[RdfResource]} renku:projectVisibility ?visibility }
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .flatMap(row => row.get("visibility").map(visibility => Visibility.apply(visibility)))
      .headOption

  private def findName(projectId: ResourceId): Name =
    runQuery(
      s"""|SELECT ?name
          |WHERE {
          |  ${projectId.showAs[RdfResource]} schema:name ?name
          |}
          |""".stripMargin
    ).unsafeRunSync()
      .flatMap(row => row.get("name").map(name => Name(name)))
      .head
}
