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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations
package projectsgraph

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.searchgraphs.projects.ProjectsGraphProvisioner
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger
import tooling._

private class ProvisionProjectsGraph[F[_]: Async: Logger](
    migrationNeedChecker:     MigrationNeedChecker[F],
    backlogCreator:           BacklogCreator[F],
    projectsFinder:           ProjectsPageFinder[F],
    progressFinder:           ProgressFinder[F],
    projectFetcher:           ProjectFetcher[F],
    projectsGraphProvisioner: ProjectsGraphProvisioner[F],
    projectDonePersister:     ProjectDonePersister[F],
    executionRegister:        MigrationExecutionRegister[F],
    recoveryStrategy:         RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ConditionedMigration[F] {

  override val exclusive: Boolean        = false
  override val name:      Migration.Name = ProvisionProjectsGraph.name

  import executionRegister._
  import fs2._
  import progressFinder._
  import projectDonePersister._
  import projectFetcher.fetchProject
  import projectsFinder._
  import projectsGraphProvisioner.provisionProjectsGraph
  import recoveryStrategy._

  protected[projectsgraph] override def required
      : EitherT[F, ProcessingRecoverableError, ConditionedMigration.MigrationRequired] = EitherT {
    migrationNeedChecker.checkMigrationNeeded
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, ConditionedMigration.MigrationRequired])
  }

  protected[projectsgraph] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    backlogCreator.createBacklog() >>
      Logger[F].info(show"$categoryName: $name backlog created") >>
      Stream
        .iterate(1)(_ + 1)
        .evalMap(_ => nextProjectsPage())
        .takeThrough(_.nonEmpty)
        .flatMap(Stream.emits(_))
        .evalMap(slug => findProgressInfo.map(slug -> _))
        .evalTap { case (slug, info) => logInfo(show"provisioning '$slug'", info) }
        .evalMap { case (slug, info) => fetchProject(slug).map(p => (slug, p, info)) }
        .evalTap { case (_, maybeProj, _) => maybeProj.map(provisionProjectsGraph).getOrElse(().pure[F]) }
        .evalTap { case (slug, _, _) => noteDone(slug) }
        .evalTap { case (slug, _, info) => logInfo(show"'$slug' provisioned", info) }
        .compile
        .drain
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(maybeRecoverableError[F, Unit])
  }

  private def logInfo(message: String, progressInfo: String): F[Unit] =
    Logger[F].info(show"${ProvisionProjectsGraph.name} - $progressInfo - $message")

  protected[projectsgraph] override def postMigration(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    registerExecution(name)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}

private[migrations] object ProvisionProjectsGraph {
  val name: Migration.Name = Migration.Name("Provision Projects Graph")

  def apply[F[_]: Async: Logger: MetricsRegistry: SparqlQueryTimeRecorder]: F[Migration[F]] = for {
    checkMigrationNeeded     <- MigrationNeedChecker[F]
    backlogCreator           <- BacklogCreator[F]
    projectsFinder           <- ProjectsPageFinder[F]
    progressFinder           <- ProgressFinder[F]
    projectFetcher           <- ProjectFetcher[F]
    projectsGraphProvisioner <- ProjectsConnectionConfig[F]().map(ProjectsGraphProvisioner[F](_))
    projectDonePersister     <- ProjectDonePersister[F]
    executionRegister        <- MigrationExecutionRegister[F]
  } yield new ProvisionProjectsGraph(checkMigrationNeeded,
                                     backlogCreator,
                                     projectsFinder,
                                     progressFinder,
                                     projectFetcher,
                                     projectsGraphProvisioner,
                                     projectDonePersister,
                                     executionRegister
  )
}
