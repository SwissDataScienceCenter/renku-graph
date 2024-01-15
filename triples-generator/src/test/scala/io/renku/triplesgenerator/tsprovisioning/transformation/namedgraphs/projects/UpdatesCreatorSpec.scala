/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.projects

import TestDataTools._
import cats.effect.std.CountDownLatch
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Spawn}
import cats.syntax.all._
import com.softwaremill.diffx.Diff
import com.softwaremill.diffx.scalatest.DiffShouldMatcher
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.Schemas.{prov, renku, schema}
import io.renku.graph.model._
import io.renku.graph.model.projects.DateCreated
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.tinytypes.syntax.all._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import monocle.Lens
import org.scalacheck.Gen
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.typelevel.log4cats.Logger

import java.time.Instant
import scala.concurrent.duration._

class UpdatesCreatorSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with EntitiesGenerators
    with DiffShouldMatcher
    with MoreDiffInstances
    with ScalaCheckPropertyChecks {

  import UpdatesCreator._

  "postUpdates" should {

    "remove duplicate created dates" in projectsDSConfig.use { implicit pcc =>
      val dateCreated = DateCreated(Instant.parse("2022-12-09T13:45:13Z"))
      val project1 = projectCreatedLens
        .replace(dateCreated)(anyProjectEntities.generateOne.to[entities.Project])
      val project2 = projectCreatedLens.modify(_ - 1.days).apply(project1)

      for {
        _ <- uploadToProjects(project1, project2)

        _ <- runUpdates(postUpdates(project1))

        _ <- findProjects.asserting { projects =>
               projects.size                  shouldBe 1
               projects.head.maybeName        shouldBe project1.name.value.some
               projects.head.maybeDateCreated shouldBe project2.dateCreated.some
             }
      } yield Succeeded
    }

    "retain any single date" in projectsDSConfig.use { implicit pcc =>
      val dateCreated = DateCreated(Instant.parse("2022-12-09T13:45:13Z"))
      val project =
        projectCreatedLens.replace(dateCreated)(
          anyProjectEntities.generateOne.to[entities.Project]
        )

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates(postUpdates(project))

        _ <- findProjects.asserting { projects =>
               projects.size                  shouldBe 1
               projects.head.maybeName        shouldBe project.name.value.some
               projects.head.maybeDateCreated shouldBe project.dateCreated.some
             }
      } yield Succeeded
    }

    "retain minimum date when delete concurrently" in projectsDSConfig.use { implicit pcc =>
      val dateCreated = DateCreated(Instant.parse("2022-12-09T13:45:13Z"))
      val project1 =
        projectCreatedLens.replace(dateCreated)(
          anyProjectEntities.generateOne.to[entities.Project]
        )
      val project2 = projectCreatedLens.modify(_ - 1.days).apply(project1)

      for {
        _ <- uploadToProjects(project1, project2)

        wait <- CountDownLatch[IO](1)
        task1 = wait.await *> runUpdates(postUpdates(project1))
        task2 = wait.await *> runUpdates(postUpdates(project2))
        run =
          for {
            fib <- Spawn[IO].start(List(task1, task2).parSequence)
            _   <- wait.release
            _   <- fib.join
          } yield ()

        _ <- run
        _ <- findProjects.asserting { projects =>
               projects.size                  shouldBe 1
               projects.head.maybeName        shouldBe project1.name.value.some
               projects.head.maybeDateCreated shouldBe project2.dateCreated.some
             }
      } yield Succeeded
    }

    "don't confuse with datasets when removing dates" in projectsDSConfig.use { implicit pcc =>
      val (dataset, project) = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceInternal))
        .generateOne
        .bimap(identity, _.to[entities.Project])

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates(postUpdates(project))
        _ <- findProjects.asserting { projects =>
               projects.size                  shouldBe 1
               projects.head.maybeName        shouldBe project.name.value.some
               projects.head.maybeDateCreated shouldBe project.dateCreated.some
             }
        _ <- findDatasets.asserting { datasets =>
               datasets.size      shouldBe 1
               datasets.head.name shouldBe dataset.identification.name.value.some
             }
      } yield Succeeded
    }
  }

  "prepareUpdates" should {
    "not delete existing images if they did not change" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities
        .suchThat(_.images.nonEmpty)
        .generateOne
        .to[entities.Project]

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates(prepareUpdates(project, toProjectMutableData(project)))

        _ <- project.images.traverse_ { im =>
               findImage(im.resourceId).asserting(_ shouldBe im.some)
             }
        _ <- findProjects.asserting(
               _.flatMap(_.images) should contain theSameElementsAs project.images.map(_.resourceId.show)
             )
      } yield Succeeded
    }

    "generate queries which delete the project images when changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.suchThat(_.images.nonEmpty).generateOne.to[entities.Project]

      for {
        _ <- uploadToProjects(project)

        _ <- findProjects
               .asserting(_.flatMap(_.images) should contain theSameElementsAs project.images.map(_.resourceId.show))

        _ <- runUpdates(prepareUpdates(project, toProjectMutableData(project).copy(images = Nil)))

        _ <- project.images.traverse_(im => findImage(im.resourceId).asserting(_ shouldBe None))
        _ <- findProjects.asserting(_.flatMap(_.images) shouldBe Set.empty)
      } yield Succeeded
    }

    "generate queries which delete the project name when changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates(prepareUpdates(project, toProjectMutableData(project).copy(name = projectNames.generateOne)))

        _ <- findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeName = None)))
      } yield Succeeded
    }

    val projectWithParentScenarios = Table(
      "project" -> "type",
      renkuProjectWithParentEntities(anyVisibility).generateOne.to[entities.RenkuProject.WithParent] ->
        "renku project",
      nonRenkuProjectWithParentEntities(anyVisibility).generateOne.to[entities.NonRenkuProject.WithParent] ->
        "non-renku project"
    )

    forAll(projectWithParentScenarios) { case (project, projectType) =>
      s"generate queries which deletes the $projectType's derivedFrom when changed" in projectsDSConfig.use {
        implicit pcc =>
          for {
            _ <- uploadToProjects(project)

            kgProjectInfo = toProjectMutableData(project).copy(maybeParentId = projectResourceIds.generateSome)

            _ <- runUpdates(prepareUpdates(project, kgProjectInfo))

            _ <- findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = None)))
          } yield Succeeded
      }
    }

    val projectWithoutParentScenarios = Table(
      "project" -> "type",
      renkuProjectEntities(anyVisibility).generateOne.to[entities.RenkuProject.WithoutParent] ->
        "renku project",
      nonRenkuProjectEntities(anyVisibility).generateOne.to[entities.NonRenkuProject.WithoutParent] ->
        "non-renku project"
    )

    forAll(projectWithoutParentScenarios) { case (project, projectType) =>
      s"generate queries which deletes the $projectType's derivedFrom when removed" in projectsDSConfig.use {
        implicit pcc =>
          for {
            _ <- uploadToProjects(project)

            parentId = projectResourceIds.generateOne
            _ <- insert(
                   Quad(GraphClass.Project.id(project.resourceId),
                        project.resourceId.asEntityId,
                        prov / "wasDerivedFrom",
                        parentId.asEntityId
                   )
                 )

            _ <-
              findProjects
                .asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = parentId.show.some)))

            _ <- runUpdates(prepareUpdates(project, toProjectMutableData(project).copy(maybeParentId = parentId.some)))

            _ <- findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = None)))
          } yield Succeeded
      }
    }

    forAll(projectWithParentScenarios) { case (project, projectType) =>
      s"not generate queries which deletes the $projectType's derivedFrom when NOT changed" in projectsDSConfig.use {
        implicit pcc =>
          uploadToProjects(project) >>
            runUpdates(prepareUpdates(project, toProjectMutableData(project))) >>
            findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project)))
      }
    }

    "generate queries which deletes the project visibility when changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project)
        .copy(visibility = Gen.oneOf(projects.Visibility.all.filterNot(_ == project.visibility)).generateOne)

      uploadToProjects(project) >>
        runUpdates(prepareUpdates(project, kgProjectInfo)) >>
        findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeVisibility = None)))
    }

    forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
      s"generate queries which deletes the project description when changed - project ${project.name}" in projectsDSConfig
        .use { implicit pcc =>
          val kgProjectInfo = toProjectMutableData(project).copy(maybeDescription = projectDescriptions.generateSome)

          uploadToProjects(project) >>
            runUpdates(prepareUpdates(project, kgProjectInfo)) >>
            findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeDesc = None)))
        }
    }

    "generate queries which deletes the project keywords when changed" in projectsDSConfig.use { implicit pcc =>
      val project       = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project).copy(keywords = projectKeywords.generateSet(min = 1))

      uploadToProjects(project) >>
        runUpdates(prepareUpdates(project, kgProjectInfo)) >>
        findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(keywords = Set.empty)))
    }

    forAll(anyRenkuProjectEntities.map(_.to[entities.RenkuProject])) { project =>
      s"generate queries which deletes the project agent when changed - project ${project.name}" in projectsDSConfig
        .use { implicit pcc =>
          val kgProjectInfo =
            toProjectMutableData(project).copy(maybeAgent = GraphModelGenerators.cliVersions.generateSome)

          uploadToProjects(project) >>
            runUpdates(prepareUpdates(project, kgProjectInfo)) >>
            findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeAgent = None)))
        }
    }

    forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
      s"generate queries which deletes the project creator when changed - project ${project.name}" in projectsDSConfig
        .use { implicit pcc =>
          val kgProjectInfo = toProjectMutableData(project).copy(maybeCreatorId = personResourceIds.generateSome)

          uploadToProjects(project) >>
            runUpdates(prepareUpdates(project, kgProjectInfo)) >>
            findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeCreatorId = None)))
        }
    }

    "not generate queries when nothing changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      uploadToProjects(project) >>
        runUpdates(prepareUpdates(project, toProjectMutableData(project))) >>
        findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project)))
    }
  }

  "dateCreatedDeletion" should {

    "generate queries which delete the project dateCreated when changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates {
               dateCreatedDeletion(
                 project,
                 toProjectMutableData(project).copy(createdDates = projectCreatedDates().generateNonEmptyList())
               )
             }

        _ <- findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeDateCreated = None)))
      } yield Succeeded
    }
  }

  "dateModifiedDeletion" should {

    "generate queries which delete the project dateModified when changed" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne.to[entities.Project]

      for {
        _ <- uploadToProjects(project)

        _ <- runUpdates {
               dateModifiedDeletion(
                 project,
                 toProjectMutableData(project)
                   .copy(modifiedDates = projectModifiedDates(project.dateCreated.value).generateNonEmptyList().toList)
               )
             }

        _ <- findProjects.asserting(_ shouldBe Set(CurrentProjectState.from(project).copy(maybeDateModified = None)))
      } yield Succeeded
    }
  }

  private case class CurrentProjectState(maybeName:         Option[String],
                                         maybeDateCreated:  Option[projects.DateCreated],
                                         maybeDateModified: Option[projects.DateModified],
                                         maybeParentId:     Option[String],
                                         maybeVisibility:   Option[String],
                                         maybeDesc:         Option[String],
                                         keywords:          Set[String],
                                         maybeAgent:        Option[String],
                                         maybeCreatorId:    Option[String],
                                         images:            Set[String]
  )

  private case class CurrentDatasetState(name: Option[String], dateCreated: Option[Instant])

  private object CurrentProjectState {
    def from(project: entities.Project): CurrentProjectState = CurrentProjectState(
      project.name.value.some,
      project.dateCreated.some,
      project.dateModified.some,
      findParent(project).map(_.value),
      project.visibility.value.some,
      project.maybeDescription.map(_.value),
      project.keywords.map(_.value),
      findAgent(project).map(_.value),
      project.maybeCreator.map(_.resourceId.value),
      project.images.map(_.resourceId.value).toSet
    )

    implicit val diff: Diff[CurrentProjectState] =
      Diff.derived[CurrentProjectState]
  }

  private def findDatasets(implicit pcc: ProjectsConnectionConfig): IO[Set[CurrentDatasetState]] =
    runSelect(
      SparqlQuery.of(
        "fetch datasets",
        Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""SELECT ?name ?dateCreated
           |WHERE {
           |  Graph ?g {
           |    ?id a schema:Dataset
           |    OPTIONAL { ?id schema:name ?name }
           |    OPTIONAL { ?id schema:dateCreated ?dateCreated }
           |  }
           |}
           |""".stripMargin
      )
    ).map(
      _.map(row =>
        CurrentDatasetState(
          name = row.get("name"),
          dateCreated = row.get("dateCreated").map(d => Instant.parse(d))
        )
      ).toSet
    )

  private def findProjects(implicit pcc: ProjectsConnectionConfig): IO[Set[CurrentProjectState]] =
    runSelect(
      SparqlQuery.of(
        "fetch project data",
        Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""|SELECT ?name ?dateCreated ?dateModified ?maybeParent ?visibility ?maybeDesc
            |  (GROUP_CONCAT(?keyword; separator=',') AS ?keywords) ?maybeAgent ?maybeCreatorId
            |  (GROUP_CONCAT(?imageId; separator=',') AS ?images)
            |WHERE {
            |  GRAPH ?id {
            |    ?id a schema:Project
            |    OPTIONAL { ?id schema:name ?name } 
            |    OPTIONAL { ?id schema:dateCreated ?dateCreated } 
            |    OPTIONAL { ?id schema:dateModified ?dateModified }
            |    OPTIONAL { ?id prov:wasDerivedFrom ?maybeParent }
            |    OPTIONAL { ?id renku:projectVisibility ?visibility } 
            |    OPTIONAL { ?id schema:description ?maybeDesc } 
            |    OPTIONAL { ?id schema:keywords ?keyword } 
            |    OPTIONAL { ?id schema:agent ?maybeAgent } 
            |    OPTIONAL { ?id schema:creator ?maybeCreatorId }
            |    OPTIONAL { ?id schema:image ?imageId }
            |  }
            |}
            |GROUP BY ?name ?dateCreated ?dateModified ?maybeParent ?visibility ?maybeDesc ?maybeAgent ?maybeCreatorId
            |""".stripMargin
      )
    ).map(
      _.map(row =>
        CurrentProjectState(
          maybeName = row.get("name"),
          maybeDateCreated = row.get("dateCreated").map(d => projects.DateCreated(Instant.parse(d))),
          maybeDateModified = row.get("dateModified").map(d => projects.DateModified(Instant.parse(d))),
          maybeParentId = row.get("maybeParent"),
          maybeVisibility = row.get("visibility"),
          maybeDesc = row.get("maybeDesc"),
          keywords = row.get("keywords").map(_.split(',').toList).sequence.flatten.toSet,
          maybeAgent = row.get("maybeAgent"),
          maybeCreatorId = row.get("maybeCreatorId"),
          images = row.get("images").map(_.split(',').toSet).getOrElse(Set.empty)
        )
      ).toSet
    )

  private def findImage(id: model.images.ImageResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Option[model.images.Image]] =
    runSelect(
      SparqlQuery.of(
        "fetch image",
        Prefixes of schema -> "schema",
        s"""|SELECT ?imageId ?uri ?position
            |WHERE {
            |  GRAPH ?id {
            |    BIND (${id.asEntityId.asSparql.sparql} AS ?imageId)
            |    ?imageId schema:contentUrl ?uri;
            |             schema:position ?position.
            |  }
            |}
            |""".stripMargin
      )
    ).map(
      _.map(row =>
        model.images.Image(
          row.get("imageId").map(model.images.ImageResourceId.apply).getOrElse(fail("No image id")),
          row.get("uri").map(model.images.ImageUri.apply).getOrElse(fail("No image uri")),
          row.get("position").map(p => model.images.ImagePosition(p.toInt)).getOrElse(fail("No image position"))
        )
      ).headOption
    )

  private def projectCreatedLens: Lens[entities.Project, DateCreated] =
    Lens[entities.Project, DateCreated](_.dateCreated) { created =>
      {
        case p: entities.NonRenkuProject.WithParent    => p.copy(dateCreated = created)
        case p: entities.NonRenkuProject.WithoutParent => p.copy(dateCreated = created)
        case p: entities.RenkuProject.WithParent       => p.copy(dateCreated = created)
        case p: entities.RenkuProject.WithoutParent    => p.copy(dateCreated = created)
      }
    }

  private implicit lazy val logger: Logger[IO] = TestLogger()
}
