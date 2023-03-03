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

package io.renku.triplesgenerator.events.consumers
package tsmigrationrequest
package migrations
package v10migration

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.circe.literal._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling._

private class MigrationToV10[F[_]: Async: Logger](
    backlogCreator:       BacklogCreator[F],
    projectsFinder:       PagedProjectsFinder[F],
    progressFinder:       ProgressFinder[F],
    envReadinessChecker:  EnvReadinessChecker[F],
    eventsSender:         EventSender[F],
    projectDonePersister: ProjectDonePersister[F],
    executionRegister:    MigrationExecutionRegister[F],
    projectsDoneCleanUp:  ProjectsDoneCleanUp[F],
    recoveryStrategy:     RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](MigrationToV10.name, executionRegister, recoveryStrategy) {

  import envReadinessChecker._
  import eventsSender._
  import executionRegister._
  import fs2._
  import progressFinder._
  import projectDonePersister._
  import projectsDoneCleanUp._
  import projectsFinder._
  import recoveryStrategy._

  private val cleanUpEventCategory = CategoryName("CLEAN_UP_REQUEST")

  protected[v10migration] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    backlogCreator.createBacklog() >>
      Stream
        .iterate(1)(_ + 1)
        .evalMap(_ => nextProjectsPage())
        .takeThrough(_.nonEmpty)
        .flatMap(in => Stream.emits(in))
        .evalMap(path => findProgressInfo.map(path -> _))
        .evalTap { case (path, info) => logInfo(show"processing project '$path'; waiting for free resources", info) }
        .evalTap(_ => envReadyToTakeEvent)
        .evalTap { case (path, info) => logInfo(show"sending $cleanUpEventCategory event for '$path'", info) }
        .evalTap(sendCleanUpEvent)
        .evalTap { case (path, _) => noteDone(path) }
        .evalTap { case (path, info) => logInfo(show"event sent for '$path'", info) }
        .compile
        .drain
        .flatMap(_ => registerExecution(name))
        .flatMap(_ => cleanUp())
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val sendCleanUpEvent: ((projects.Path, String)) => F[Unit] = { case (path, _) =>
    val (payload, ctx) = toCleanUpEvent(path)
    sendEvent(payload, ctx)
  }

  private def toCleanUpEvent(path: projects.Path): (EventRequestContent.NoPayload, EventSender.EventContext) =
    EventRequestContent.NoPayload {
      json"""{
        "categoryName": $cleanUpEventCategory,
        "project": {
          "path": $path
        }
      }"""
    } -> EventSender.EventContext(cleanUpEventCategory, show"$categoryName: $name cannot send event for $path")

  private def logInfo(message: String, progressInfo: String): F[Unit] =
    Logger[F].info(show"${MigrationToV10.name} - $progressInfo - $message")
}

private[migrations] object MigrationToV10 {
  val name: Migration.Name = Migration.Name("Migration to V10")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    backlogCreator       <- BacklogCreator[F]
    projectsFinder       <- PagedProjectsFinder[F]
    progressFinder       <- ProgressFinder[F]
    envReadinessChecker  <- EnvReadinessChecker[F]
    eventsSender         <- EventSender[F](EventLogUrl)
    projectDonePersister <- ProjectDonePersister[F]
    executionRegister    <- MigrationExecutionRegister[F]
    projectsDoneCleanUp  <- ProjectsDoneCleanUp[F]
  } yield new MigrationToV10(backlogCreator,
                             projectsFinder,
                             progressFinder,
                             envReadinessChecker,
                             eventsSender,
                             projectDonePersister,
                             executionRegister,
                             projectsDoneCleanUp
  )
}
