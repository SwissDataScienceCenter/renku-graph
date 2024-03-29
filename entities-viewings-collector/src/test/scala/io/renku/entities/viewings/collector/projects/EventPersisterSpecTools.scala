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

package io.renku.entities.viewings.collector.projects

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector.ProjectViewedTimeOntology.dataViewedProperty
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.{GraphClass, entities, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

import java.time.Instant

trait EventPersisterSpecTools {
  self: TestProjectsDataset with AsyncIOSpec =>

  protected def findAllViewings(implicit pcc: ProjectsConnectionConfig, L: Logger[IO]) =
    runSelect(
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        sparql"""|SELECT DISTINCT ?id ?date
                 |FROM ${GraphClass.ProjectViewedTimes.id} {
                 |  ?id renku:dateViewed ?date.
                 |}
                 |""".stripMargin
      )
    ).map(_.map(row => projects.ResourceId(row("id")) -> projects.DateViewed(Instant.parse(row("date")))).toSet)

  protected def insertOtherDate(project: entities.Project, dateViewed: projects.DateViewed)(implicit
      pcc: ProjectsConnectionConfig,
      L:   Logger[IO]
  ): IO[Unit] =
    insert(
      Quad(GraphClass.ProjectViewedTimes.id, project.resourceId.asEntityId, dataViewedProperty.id, dateViewed.asObject)
    )
}
