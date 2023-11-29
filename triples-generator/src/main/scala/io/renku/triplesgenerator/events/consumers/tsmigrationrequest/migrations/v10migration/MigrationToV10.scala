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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
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
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling._

private class MigrationToV10[F[_]: Async: Logger](
    migrationNeedChecker: MigrationNeedChecker[F],
    backlogCreator:       BacklogCreator[F],
    projectsFinder:       ProjectsPageFinder[F],
    progressFinder:       ProgressFinder[F],
    envReadinessChecker:  EnvReadinessChecker[F],
    eventsSender:         EventSender[F],
    projectDonePersister: ProjectDonePersister[F],
    executionRegister:    MigrationExecutionRegister[F],
    recoveryStrategy:     RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ConditionedMigration[F] {

  override val exclusive: Boolean        = true
  override val name:      Migration.Name = MigrationToV10.name

  import envReadinessChecker._
  import eventsSender._
  import executionRegister._
  import fs2._
  import progressFinder._
  import projectDonePersister._
  import projectsFinder._
  import recoveryStrategy._

  protected[v10migration] override def required
      : EitherT[F, ProcessingRecoverableError, ConditionedMigration.MigrationRequired] = EitherT {
    migrationNeedChecker.checkMigrationNeeded
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, ConditionedMigration.MigrationRequired])
  }

  private val cleanUpEventCategory = CategoryName("CLEAN_UP_REQUEST")

  protected[v10migration] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    backlogCreator.createBacklog() >>
      Logger[F].info(show"$categoryName: $name backlog created") >>
      Stream
        .iterate(1)(_ + 1)
        .evalMap(_ => nextProjectsPage())
        .takeThrough(_.nonEmpty)
        .flatMap(in => Stream.emits(in))
        .evalMap(slug => findProgressInfo.map(slug -> _))
        .evalTap { case (slug, info) => logInfo(show"processing project '$slug'; waiting for free resources", info) }
        .evalTap(_ => envReadyToTakeEvent)
        .evalTap { case (slug, info) => logInfo(show"sending $cleanUpEventCategory event for '$slug'", info) }
        .evalTap(sendCleanUpEvent)
        .evalTap { case (slug, _) => noteDone(slug) }
        .evalTap { case (slug, info) => logInfo(show"event sent for '$slug'", info) }
        .compile
        .drain
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val sendCleanUpEvent: ((projects.Slug, String)) => F[Unit] = { case (slug, _) =>
    val (payload, ctx) = toCleanUpEvent(slug)
    sendEvent(payload, ctx)
  }

  private def toCleanUpEvent(slug: projects.Slug): (EventRequestContent.NoPayload, EventSender.EventContext) =
    EventRequestContent.NoPayload {
      json"""{
        "categoryName": $cleanUpEventCategory,
        "project": {
          "slug": $slug
        }
      }"""
    } -> EventSender.EventContext(cleanUpEventCategory, show"$categoryName: $name cannot send event for $slug")

  private def logInfo(message: String, progressInfo: String): F[Unit] =
    Logger[F].info(show"${MigrationToV10.name} - $progressInfo - $message")

  protected[v10migration] override def postMigration(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    registerExecution(name)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}

private[migrations] object MigrationToV10 {
  val name: Migration.Name = Migration.Name("Migration to V10")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    checkMigrationNeeded <- MigrationNeedChecker[F]
    backlogCreator       <- BacklogCreator[F]
    projectsFinder       <- ProjectsPageFinder[F]
    progressFinder       <- ProgressFinder[F]
    envReadinessChecker  <- EnvReadinessChecker[F]
    eventsSender         <- EventSender[F](EventLogUrl)
    projectDonePersister <- ProjectDonePersister[F]
    executionRegister    <- MigrationExecutionRegister[F]
  } yield new MigrationToV10(checkMigrationNeeded,
                             backlogCreator,
                             projectsFinder,
                             progressFinder,
                             envReadinessChecker,
                             eventsSender,
                             projectDonePersister,
                             executionRegister
  )
}
