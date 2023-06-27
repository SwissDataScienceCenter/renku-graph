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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.datemodified

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.GraphClass
import io.renku.graph.model.Schemas.schema
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait DatePersister[F[_]] {
  def persistDateModified(projectInfo: ProjectInfo): F[Unit]
}

private object DatePersister {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): DatePersister[F] = new DatePersisterImpl[F](TSClient[F](connectionConfig))
}

private class DatePersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends DatePersister[F] {

  override def persistDateModified(projectInfo: ProjectInfo): F[Unit] =
    insertDates(projectInfo) >> deduplicateDates(projectInfo)

  private lazy val insertDates: ProjectInfo => F[Unit] = { case ProjectInfo(id, _, date) =>
    tsClient.updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${AddProjectDateModified.name} - insert dateModified",
        Prefixes of schema -> "schema",
        sparql"""|INSERT DATA {
                 |  GRAPH ${GraphClass.Projects.id} { ${id.asEntityId} schema:dateModified ${date.asObject} }
                 |  GRAPH ${id.asEntityId} { ${id.asEntityId} schema:dateModified ${date.asObject} }
                 |}
                 |""".stripMargin
      )
    )
  }

  private def deduplicateDates(p: ProjectInfo): F[Unit] =
    deduplicateDatesInProject(p) >> deduplicateDatesInProjects(p)

  private lazy val deduplicateDatesInProject: ProjectInfo => F[Unit] = { case ProjectInfo(id, _, _) =>
    tsClient.updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${AddProjectDateModified.name} - deduplicate Project dateModified",
        Prefixes of schema -> "schema",
        sparql"""|DELETE {
                 |  GRAPH ?id { ?id schema:dateModified ?date }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ?id {
                 |    {
                 |      SELECT ?id (MAX(?date) AS ?maxDate)
                 |      WHERE {
                 |        ?id schema:dateModified ?date
                 |      }
                 |      GROUP BY ?id
                 |      HAVING (COUNT(?date) > 1)
                 |    }
                 |    ?id schema:dateModified ?date.
                 |    FILTER (?date != ?maxDate)
                 |  }
                 |}
                 |""".stripMargin
      )
    )
  }

  private lazy val deduplicateDatesInProjects: ProjectInfo => F[Unit] = { case ProjectInfo(id, _, _) =>
    tsClient.updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${AddProjectDateModified.name} - deduplicate Projects dateModified",
        Prefixes of schema -> "schema",
        sparql"""|DELETE {
                 |  GRAPH ${GraphClass.Projects.id} { ?id schema:dateModified ?date }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ${GraphClass.Projects.id} {
                 |    {
                 |      SELECT ?id (MAX(?date) AS ?maxDate)
                 |      WHERE {
                 |        ?id schema:dateModified ?date
                 |      }
                 |      GROUP BY ?id
                 |      HAVING (COUNT(?date) > 1)
                 |    }
                 |    ?id schema:dateModified ?date.
                 |    FILTER (?date != ?maxDate)
                 |  }
                 |}
                 |""".stripMargin
      )
    )
  }
}
