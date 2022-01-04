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

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {
  import UpdatesCreator._

  "prepareUpdates" should {
    "generate queries which deletes the project name when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo =
        (projectNames.generateOne, project.maybeParent, project.visibility, project.maybeDescription, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (None,
         project.maybeParent.map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }

    "generate queries which deletes the project's derivedFrom when changed" in {
      val project = projectWithParentEntities(anyVisibility).generateOne.to[entities.ProjectWithParent]
      val kgProjectInfo =
        (project.name, projectResourceIds.generateSome, project.visibility, project.maybeDescription, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         None,
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }

    "generate queries which deletes the project's derivedFrom when removed" in {
      val project       = projectWithParentEntities(anyVisibility).generateOne.to[entities.ProjectWithParent]
      val kgProjectInfo = (project.name, None, project.visibility, project.maybeDescription, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         None,
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }

    "not generate queries which deletes the project's derivedFrom when NOT changed" in {
      val project = projectWithParentEntities(anyVisibility).generateOne.to[entities.ProjectWithParent]
      val kgProjectInfo =
        (project.name, project.maybeParent, project.visibility, project.maybeDescription, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         project.maybeParent.map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }

    "generate queries which deletes the project visibility when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo = (project.name,
                           project.maybeParent,
                           Gen.oneOf(projects.Visibility.all.filterNot(_ == project.visibility)).generateOne,
                           project.maybeDescription,
                           project.keywords
      )

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         project.maybeParent.map(_.value),
         None,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }

    "generate queries which deletes the project description when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo =
        (project.name, project.maybeParent, project.visibility, projectDescriptions.generateSome, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         project.maybeParent.map(_.value),
         project.visibility.value.some,
         None,
         project.keywords.map(_.value)
        )
      )
    }

    "generate queries which deletes the project keywords when changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo =
        (project.name,
         project.maybeParent,
         project.visibility,
         project.maybeDescription,
         projectKeywords.generateSet(minElements = 1)
        )

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         project.maybeParent.map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         Set.empty
        )
      )
    }

    "not generate queries when nothing changed" in {
      val project = anyProjectEntities.generateOne.to[entities.Project]
      val kgProjectInfo =
        (project.name, project.maybeParent, project.visibility, project.maybeDescription, project.keywords)

      loadToStore(project)

      val queries = prepareUpdates(project, kgProjectInfo)

      queries.runAll.unsafeRunSync()

      findProjects shouldBe Set(
        (project.name.value.some,
         project.maybeParent.map(_.value),
         project.visibility.value.some,
         project.maybeDescription.map(_.value),
         project.keywords.map(_.value)
        )
      )
    }
  }

  private implicit class ProjectOps(project: entities.Project) {
    val maybeParent = project match {
      case projectWithParent: entities.ProjectWithParent    => Some(projectWithParent.parentResourceId)
      case _:                 entities.ProjectWithoutParent => None
    }
  }

  private def findProjects: Set[(Option[String], Option[String], Option[String], Option[String], Set[String])] =
    runQuery(
      s"""|SELECT ?name ?maybeParent ?visibility ?description (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
          |WHERE {
          |  ?id a schema:Project .
          |  OPTIONAL { ?id schema:name ?name } 
          |  OPTIONAL { ?id prov:wasDerivedFrom ?maybeParent } 
          |  OPTIONAL { ?id renku:projectVisibility ?visibility } 
          |  OPTIONAL { ?id schema:description ?description } 
          |  OPTIONAL { ?id schema:keywords ?keyword } 
          |}
          |GROUP BY ?name ?maybeParent ?visibility ?description
          |""".stripMargin
    )
      .unsafeRunSync()
      .map(row =>
        (row.get("name"),
         row.get("maybeParent"),
         row.get("visibility"),
         row.get("description"),
         row.get("keywords").map(_.split(',').toList).sequence.flatten.toSet
        )
      )
      .toSet
}
