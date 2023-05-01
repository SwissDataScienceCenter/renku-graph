/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.entities.viewings.collector.persons

import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.{GraphClass, entities, persons, projects}
import io.renku.jsonld.syntax._
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._

import java.time.Instant

trait PersonViewedProjectSpecTools {
  self: InMemoryJenaForSpec with ProjectsDataset with IOSpec =>

  protected def findAllViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find user project viewings",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?id ?projectId ?date
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  ?id renku:viewedProject ?viewingId.
                 |  ?viewingId renku:project ?projectId;
                 |             renku:dateViewed ?date.
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        ViewingRecord(persons.ResourceId(row("id")),
                      projects.ResourceId(row("projectId")),
                      projects.DateViewed(Instant.parse(row("date")))
        )
      )
      .toSet

  protected case class ViewingRecord(userId:    persons.ResourceId,
                                     projectId: projects.ResourceId,
                                     date:      projects.DateViewed
  )

  protected def toCollectorProject(project: entities.Project) =
    collector.persons.Project(project.resourceId, project.path)

  protected def insertOtherDate(projectId: projects.ResourceId, dateViewed: projects.DateViewed) =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.of(
        "test add another user project dateViewed",
        Prefixes of renku -> "renku",
        sparql"""|INSERT {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId renku:dateViewed ${dateViewed.asObject}
                 |  }
                 |}
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId renku:project ${projectId.asEntityId}
                 |  }
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
}
