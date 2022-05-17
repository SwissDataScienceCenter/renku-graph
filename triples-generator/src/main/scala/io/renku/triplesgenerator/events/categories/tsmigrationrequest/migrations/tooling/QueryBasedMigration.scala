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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations.tooling

import QueryBasedMigration.EventData
import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[migrations] class QueryBasedMigration[F[_]: MonadThrow: Logger](
    override val name: Migration.Name,
    projectsFinder:    ProjectsFinder[F],
    eventProducer:     projects.Path => EventData,
    eventSender:       EventSender[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](name, executionRegister, recoveryStrategy) {

  import projectsFinder._
  import recoveryStrategy._

  protected[tooling] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findProjects().map(toEvents) >>= sendEvents)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  lazy val toEvents: List[projects.Path] => List[EventData] = _.map(eventProducer)

  lazy val sendEvents: List[EventData] => F[Unit] = _.map { case (path, event, eventCategory) =>
    eventSender.sendEvent(
      event,
      EventSender.EventContext(eventCategory, show"$categoryName: $name cannot send event for $path")
    )
  }.sequence.void
}

private[migrations] object QueryBasedMigration {

  type EventData = (projects.Path, EventRequestContent.NoPayload, CategoryName)

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder: MetricsRegistry](
      name:          Migration.Name,
      query:         SparqlQuery,
      eventProducer: projects.Path => EventData
  ): F[QueryBasedMigration[F]] = for {
    projectsFinder    <- ProjectsFinder[F](query)
    eventSender       <- EventSender[F]
    executionRegister <- MigrationExecutionRegister[F]
  } yield new QueryBasedMigration[F](name, projectsFinder, eventProducer, eventSender, executionRegister)
}
