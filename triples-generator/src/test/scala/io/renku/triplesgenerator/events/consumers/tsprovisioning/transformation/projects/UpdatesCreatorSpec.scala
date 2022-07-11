/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.projects

import TestDataTools._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQuery}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with TableDrivenPropertyChecks
    with ScalaCheckPropertyChecks {
  import UpdatesCreator._

  "prepareUpdates" should {

    "generate queries which delete the project name when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      upload(to = renkuDataset, project)

      prepareUpdates(project, toProjectMutableData(project).copy(name = projectNames.generateOne))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeName = None))
    }

    val projectWithParentScenarios = Table(
      "project" -> "type",
      renkuProjectWithParentEntities(anyVisibility).generateOne.to[entities.RenkuProject.WithParent] ->
        "renku project",
      nonRenkuProjectWithParentEntities(anyVisibility).generateOne.to[entities.NonRenkuProject.WithParent] ->
        "non-renku project"
    )

    forAll(projectWithParentScenarios) { case (project, projectType) =>
      s"generate queries which deletes the $projectType's derivedFrom when changed" in {

        upload(to = renkuDataset, project)

        val kgProjectInfo = toProjectMutableData(project).copy(maybeParentId = projectResourceIds.generateSome)

        prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = None))
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
      s"generate queries which deletes the $projectType's derivedFrom when removed" in {

        upload(to = renkuDataset, project)

        val parentId = projectResourceIds.generateOne
        upload(to = renkuDataset,
               JsonLD.edge(project.resourceId.asEntityId, prov / "wasDerivedFrom", parentId.asEntityId)
        )

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = parentId.show.some))

        val kgProjectInfo = toProjectMutableData(project).copy(maybeParentId = parentId.some)

        prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeParentId = None))
      }
    }

    forAll(projectWithParentScenarios) { case (project, projectType) =>
      s"not generate queries which deletes the $projectType's derivedFrom when NOT changed" in {

        upload(to = renkuDataset, project)

        prepareUpdates(project, toProjectMutableData(project)).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project))
      }
    }

    "generate queries which deletes the project visibility when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project)
        .copy(visibility = Gen.oneOf(projects.Visibility.all.filterNot(_ == project.visibility)).generateOne)

      upload(to = renkuDataset, project)

      prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

      findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeVisibility = None))
    }

    "generate queries which deletes the project description when changed" in {
      forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
        val kgProjectInfo = toProjectMutableData(project).copy(maybeDescription = projectDescriptions.generateSome)

        upload(to = renkuDataset, project)

        prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeDesc = None))

        clear(renkuDataset)
      }
    }

    "generate queries which deletes the project keywords when changed" in {
      val project       = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project).copy(keywords = projectKeywords.generateSet(minElements = 1))

      upload(to = renkuDataset, project)

      prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

      findProjects shouldBe Set(CurrentProjectState.from(project).copy(keywords = Set.empty))
    }

    "generate queries which deletes the project agent when changed" in {
      forAll(anyRenkuProjectEntities.map(_.to[entities.RenkuProject])) { project =>
        val kgProjectInfo = toProjectMutableData(project).copy(maybeAgent = cliVersions.generateSome)

        upload(to = renkuDataset, project)

        prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeAgent = None))

        clear(renkuDataset)
      }
    }

    "generate queries which deletes the project creator when changed" in {
      forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
        val kgProjectInfo = toProjectMutableData(project).copy(maybeCreatorId = personResourceIds.generateSome)

        upload(to = renkuDataset, project)

        prepareUpdates(project, kgProjectInfo).runAll(on = renkuDataset).unsafeRunSync()

        findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeCreatorId = None))

        clear(renkuDataset)
      }
    }

    "not generate queries when nothing changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      upload(to = renkuDataset, project)

      prepareUpdates(project, toProjectMutableData(project)).runAll(on = renkuDataset).unsafeRunSync()

      findProjects shouldBe Set(CurrentProjectState.from(project))
    }
  }

  "dateCreatedDeletion" should {

    "generate queries which delete the project dateCreated when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      upload(to = renkuDataset, project)

      dateCreatedDeletion(project, toProjectMutableData(project).copy(dateCreated = projectCreatedDates().generateOne))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findProjects shouldBe Set(CurrentProjectState.from(project).copy(maybeDateCreated = None))
    }
  }

  private case class CurrentProjectState(maybeName:        Option[String],
                                         maybeDateCreated: Option[projects.DateCreated],
                                         maybeParentId:    Option[String],
                                         maybeVisibility:  Option[String],
                                         maybeDesc:        Option[String],
                                         keywords:         Set[String],
                                         maybeAgent:       Option[String],
                                         maybeCreatorId:   Option[String]
  )

  private object CurrentProjectState {
    def from(project: entities.Project): CurrentProjectState = CurrentProjectState(
      project.name.value.some,
      project.dateCreated.some,
      findParent(project).map(_.value),
      project.visibility.value.some,
      project.maybeDescription.map(_.value),
      project.keywords.map(_.value),
      findAgent(project).map(_.value),
      project.maybeCreator.map(_.resourceId.value)
    )
  }

  private def findProjects: Set[CurrentProjectState] = runSelect(
    on = renkuDataset,
    SparqlQuery.of(
      "fetch project data",
      Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
      s"""|SELECT ?name ?dateCreated ?maybeParent ?visibility ?maybeDesc
          |  (GROUP_CONCAT(?keyword; separator=',') AS ?keywords) ?maybeAgent ?maybeCreatorId
          |WHERE {
          |  ?id a schema:Project
          |  OPTIONAL { ?id schema:name ?name } 
          |  OPTIONAL { ?id schema:dateCreated ?dateCreated } 
          |  OPTIONAL { ?id prov:wasDerivedFrom ?maybeParent } 
          |  OPTIONAL { ?id renku:projectVisibility ?visibility } 
          |  OPTIONAL { ?id schema:description ?maybeDesc } 
          |  OPTIONAL { ?id schema:keywords ?keyword } 
          |  OPTIONAL { ?id schema:agent ?maybeAgent } 
          |  OPTIONAL { ?id schema:creator ?maybeCreatorId } 
          |}
          |GROUP BY ?name ?dateCreated ?maybeParent ?visibility ?maybeDesc ?maybeAgent ?maybeCreatorId
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row =>
      CurrentProjectState(
        maybeName = row.get("name"),
        maybeDateCreated = row.get("dateCreated").map(d => projects.DateCreated(Instant.parse(d))),
        maybeParentId = row.get("maybeParent"),
        maybeVisibility = row.get("visibility"),
        maybeDesc = row.get("maybeDesc"),
        keywords = row.get("keywords").map(_.split(',').toList).sequence.flatten.toSet,
        maybeAgent = row.get("maybeAgent"),
        maybeCreatorId = row.get("maybeCreatorId")
      )
    )
    .toSet
}
