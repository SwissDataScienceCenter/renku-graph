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
package lucenereindex

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.events.CategoryName
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling._

private class ReindexLucene[F[_]: Async: Logger](
    backlogCreator:       BacklogCreator[F],
    projectsFinder:       ProjectsPageFinder[F],
    progressFinder:       ProgressFinder[F],
    envReadinessChecker:  EnvReadinessChecker[F],
    elClient:             eventlog.api.events.Client[F],
    projectDonePersister: ProjectDonePersister[F],
    executionRegister:    MigrationExecutionRegister[F],
    recoveryStrategy:     RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends RegisteredMigration[F](ReindexLucene.name, executionRegister, recoveryStrategy) {

  import envReadinessChecker._
  import fs2._
  import progressFinder._
  import projectDonePersister._
  import projectsFinder._
  import recoveryStrategy._

  private val cleanUpEventCategory = CategoryName("CLEAN_UP_REQUEST")

  protected[lucenereindex] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
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
        .evalTap(sendRedoProjectTransformation)
        .evalTap { case (slug, _) => noteDone(slug) }
        .evalTap { case (slug, info) => logInfo(show"event sent for '$slug'", info) }
        .compile
        .drain
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError[F, Unit])
  }

  private lazy val sendRedoProjectTransformation: ((projects.Slug, String)) => F[Unit] = { case (slug, _) =>
    elClient.send(StatusChangeEvent.RedoProjectTransformation(slug))
  }

  private def logInfo(message: String, progressInfo: String): F[Unit] =
    Logger[F].info(show"${ReindexLucene.name} - $progressInfo - $message")
}

private[migrations] object ReindexLucene {
  val name: Migration.Name = Migration.Name("Reindex Lucene")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    backlogCreator       <- BacklogCreator[F]
    projectsFinder       <- ProjectsPageFinder[F]
    progressFinder       <- ProgressFinder[F]
    envReadinessChecker  <- EnvReadinessChecker[F]
    elClient             <- eventlog.api.events.Client[F]
    projectDonePersister <- ProjectDonePersister[F]
    executionRegister    <- MigrationExecutionRegister[F]
  } yield new ReindexLucene(backlogCreator,
                            projectsFinder,
                            progressFinder,
                            envReadinessChecker,
                            elClient,
                            projectDonePersister,
                            executionRegister
  )
}
