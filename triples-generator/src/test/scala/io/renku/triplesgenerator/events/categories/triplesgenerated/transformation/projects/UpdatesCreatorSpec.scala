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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.projects

import TestDataTools._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.jsonld.JsonLD
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import io.renku.jsonld.syntax._

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryRdfStore
    with should.Matchers
    with TableDrivenPropertyChecks {
  import UpdatesCreator._

  "prepareUpdates" should {
    "generate queries which delete the project name when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      loadToStore(project)

      prepareUpdates(project, toProjectMutableData(project).copy(name = projectNames.generateOne)).runAll
        .unsafeRunSync()

      findProjects shouldBe Set(
        (None,
         findParent(project).map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value),
         findAgent(project).map(_.value)
        )
      )
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

        loadToStore(project)

        val kgProjectInfo = toProjectMutableData(project).copy(maybeParentId = projectResourceIds.generateSome)

        prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

        findProjects shouldBe Set(
          (project.name.value.some,
           None,
           project.visibility.value.some,
           project.maybeDescription.map(_.value),
           project.keywords.map(_.value),
           findAgent(project).map(_.value)
          )
        )
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

        loadToStore(project)

        val parentId = projectResourceIds.generateOne
        loadToStore(JsonLD.edge(project.resourceId.asEntityId, prov / "wasDerivedFrom", parentId.asEntityId))

        findProjects shouldBe Set(
          (project.name.value.some,
           parentId.value.some,
           project.visibility.value.some,
           project.maybeDescription.map(_.value),
           project.keywords.map(_.value),
           findAgent(project).map(_.value)
          )
        )

        val kgProjectInfo = toProjectMutableData(project).copy(maybeParentId = parentId.some)

        prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

        findProjects shouldBe Set(
          (project.name.value.some,
           None,
           project.visibility.value.some,
           project.maybeDescription.map(_.value),
           project.keywords.map(_.value),
           findAgent(project).map(_.value)
          )
        )
      }
    }

    forAll(projectWithParentScenarios) { case (project, projectType) =>
      s"not generate queries which deletes the $projectType's derivedFrom when NOT changed" in {

        loadToStore(project)

        prepareUpdates(project, toProjectMutableData(project)).runAll.unsafeRunSync()

        findProjects shouldBe Set(
          (project.name.value.some,
           findParent(project).map(_.value),
           project.visibility.value.some,
           project.maybeDescription.map(_.value),
           project.keywords.map(_.value),
           findAgent(project).map(_.value)
          )
        )
      }
    }

    "generate queries which deletes the project visibility when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project)
        .copy(visibility = Gen.oneOf(projects.Visibility.all.filterNot(_ == project.visibility)).generateOne)

      loadToStore(project)

      prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         findParent(project).map(_.value),
         None,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value),
         findAgent(project).map(_.value)
        )
      )
    }

    "generate queries which deletes the project description when changed" in {
      val project       = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project).copy(maybeDescription = projectDescriptions.generateSome)

      loadToStore(project)

      prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         findParent(project).map(_.value),
         project.visibility.value.some,
         None,
         project.keywords.map(_.value),
         findAgent(project).map(_.value)
        )
      )
    }

    "generate queries which deletes the project keywords when changed" in {
      val project       = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = toProjectMutableData(project).copy(keywords = projectKeywords.generateSet(minElements = 1))

      loadToStore(project)

      prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         findParent(project).map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         Set.empty,
         findAgent(project).map(_.value)
        )
      )
    }

    "generate queries which deletes the project agent when changed" in {
      val project       = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val kgProjectInfo = toProjectMutableData(project).copy(maybeAgent = cliVersions.generateSome)

      loadToStore(project)

      prepareUpdates(project, kgProjectInfo).runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         findParent(project).map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value),
         None
        )
      )
    }

    "not generate queries when nothing changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      loadToStore(project)

      prepareUpdates(project, toProjectMutableData(project)).runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         findParent(project).map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value),
         findAgent(project).map(_.value)
        )
      )
    }
  }

  private def findProjects
      : Set[(Option[String], Option[String], Option[String], Option[String], Set[String], Option[String])] =
    runQuery(
      s"""|SELECT ?name ?maybeParent ?visibility ?maybeDesc 
          |  (GROUP_CONCAT(?keyword; separator=',') AS ?keywords) ?maybeAgent
          |WHERE {
          |  ?id a schema:Project
          |  OPTIONAL { ?id schema:name ?name } 
          |  OPTIONAL { ?id prov:wasDerivedFrom ?maybeParent } 
          |  OPTIONAL { ?id renku:projectVisibility ?visibility } 
          |  OPTIONAL { ?id schema:description ?maybeDesc } 
          |  OPTIONAL { ?id schema:keywords ?keyword } 
          |  OPTIONAL { ?id schema:agent ?maybeAgent } 
          |}
          |GROUP BY ?name ?maybeParent ?visibility ?maybeDesc ?maybeAgent
          |""".stripMargin
    )
      .unsafeRunSync()
      .map(row =>
        (row.get("name"),
         row.get("maybeParent"),
         row.get("visibility"),
         row.get("maybeDesc"),
         row.get("keywords").map(_.split(',').toList).sequence.flatten.toSet,
         row.get("maybeAgent")
        )
      )
      .toSet
}
