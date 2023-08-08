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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import fs2.Stream
import io.circe.Decoder
import io.renku.eventlog.api.EventLogClient
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectViewedEvent
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.Migration
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling.{MigrationExecutionRegister, RecoverableErrorsRecovery, RegisteredMigration}

private class ProjectsDateViewedCreator[F[_]: Async: Logger](
    tsClient:          TSClient[F],
    elClient:          EventLogClient[F],
    tgClient:          triplesgenerator.api.events.Client[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](ProjectsDateViewedCreator.name, executionRegister, recoveryStrategy) {

  import recoveryStrategy._
  private val pageSize = 20

  protected[migrations] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    Stream
      .iterate(1)(_ + 1)
      .evalMap(findEventsPage)
      .takeThrough(_.nonEmpty)
      .flatMap(in => Stream.emits(in))
      .evalMap(updateDateWithLatestEvent)
      .evalMap(tgClient.send)
      .compile
      .drain
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  private def findEventsPage(page: Int) =
    tsClient.queryExpecting[List[ProjectViewedEvent]](
      SparqlQuery.ofUnsafe(
        show"$name - find projects",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|SELECT DISTINCT ?slug ?date
            |WHERE {
            |  GRAPH ?id {
            |    ?id a schema:Project;
            |        renku:projectPath ?slug;
            |        schema:dateCreated ?date.
            |  }
            |}
            |ORDER BY ?date
            |OFFSET ${(page - 1) * pageSize}
            |LIMIT $pageSize
            |""".stripMargin
      )
    )

  private implicit lazy val toEvent: Decoder[List[ProjectViewedEvent]] = ResultsDecoder[List, ProjectViewedEvent] {
    implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[projects.Slug]("slug") -> extract[projects.DateViewed]("date"))
        .mapN(ProjectViewedEvent(_, _, maybeUserId = None))
  }

  private def updateDateWithLatestEvent(event: ProjectViewedEvent): F[ProjectViewedEvent] = {
    import EventLogClient._
    elClient
      .getEvents(
        SearchCriteria
          .forProject(event.slug)
          .sortBy(SearchCriteria.Sort.EventDateDesc)
          .withPerPage(PerPage(1))
      )
      .map(
        _.toEither.toOption
          .flatMap(_.headOption)
          .map(ev => event.copy(dateViewed = projects.DateViewed(ev.eventDate.value)))
          .getOrElse(event)
      )
  }
}

private[migrations] object ProjectsDateViewedCreator {

  val name: Migration.Name = Migration.Name("Projects DateViewed creator")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] =
    for {
      tsClient          <- ProjectsConnectionConfig[F]().map(TSClient[F](_))
      elClient          <- EventLogClient[F]
      tgClient          <- triplesgenerator.api.events.Client[F]
      executionRegister <- MigrationExecutionRegister[F]
    } yield new ProjectsDateViewedCreator[F](tsClient, elClient, tgClient, executionRegister, RecoverableErrorsRecovery)
}
