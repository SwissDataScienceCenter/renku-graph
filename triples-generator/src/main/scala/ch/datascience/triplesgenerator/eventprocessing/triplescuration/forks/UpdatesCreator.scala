/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package forks

import cats.MonadError
import cats.data.{EitherT, OptionT}
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.users
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private[triplescuration] trait UpdatesCreator[Interpretation[_]] {
  def create(commit:             CommitEvent)(
      implicit maybeAccessToken: Option[AccessToken]
  ): CurationUpdatesGroup[Interpretation]
}

private[triplescuration] class UpdatesCreatorImpl(
    gitLab:              GitLabInfoFinder[IO],
    kg:                  KGInfoFinder[IO],
    updatesQueryCreator: UpdatesQueryCreator
)(implicit ME:           MonadError[IO, Throwable], cs: ContextShift[IO])
    extends UpdatesCreator[IO] {

  import updatesQueryCreator._

  override def create(
      commit:                  CommitEvent
  )(implicit maybeAccessToken: Option[AccessToken]): CurationUpdatesGroup[IO] =
    CurationUpdatesGroup[IO](
      "Fork info updates",
      () =>
        EitherT {
          gitLab
            .findProject(commit.project.path)
            .flatMap { forkInfoUpdates }
            .map { _.asRight[ProcessingRecoverableError] }
            .recover(maybeToRecoverableError)
        }
    )

  private lazy val forkInfoUpdates: Option[GitLabProject] => IO[List[SparqlQuery]] = {
    case `when creator has an email`(creatorEmail, gitLabProject) =>
      OptionT(kg.findCreatorId(creatorEmail))
        .map { existingUserResource =>
          updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
            swapCreator(gitLabProject.path, existingUserResource) ++
            recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)
        }
        .getOrElse {
          updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
            addNewCreator(gitLabProject.path, gitLabProject.maybeEmail, gitLabProject.maybeName) ++
            recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)
        }
    case `when creator has no email`(gitLabProject) =>
      (updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
        addNewCreator(gitLabProject.path, gitLabProject.maybeEmail, gitLabProject.maybeName) ++
        recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)).pure[IO]
    case _ => List.empty[SparqlQuery].pure[IO]
  }

  private object `when creator has an email` {
    def unapply(maybeProject: Option[GitLabProject]): Option[(users.Email, GitLabProject)] = maybeProject match {
      case Some(gitLabProject) => gitLabProject.maybeEmail.map(_ -> gitLabProject)
      case _                   => None
    }
  }

  private object `when creator has no email` {
    def unapply(maybeProject: Option[GitLabProject]): Option[GitLabProject] = maybeProject match {
      case Some(gitLabProject) if gitLabProject.maybeEmail.isEmpty => Some(gitLabProject)
      case _                                                       => None
    }
  }

  private implicit class GitLabProjectOps(gitLabProject: GitLabProject) {

    lazy val maybeEmail = gitLabProject.maybeCreator.flatMap(_.maybeEmail)
    lazy val maybeName  = gitLabProject.maybeCreator.flatMap(_.maybeName)
  }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, List[SparqlQuery]]] = {
    case e: UnexpectedResponseException =>
      Left[ProcessingRecoverableError, List[SparqlQuery]](
        CurationRecoverableError("Problem with finding fork info", e)
      )
    case e: ConnectivityException =>
      Left[ProcessingRecoverableError, List[SparqlQuery]](
        CurationRecoverableError("Problem with finding fork info", e)
      )
  }
}

private[triplescuration] object IOUpdateFunctionsCreator {
  import cats.effect.Timer
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[UpdatesCreator[IO]] =
    for {
      renkuBaseUrl     <- RenkuBaseUrl[IO]()
      gitLabInfoFinder <- IOGitLabInfoFinder(gitLabThrottler, logger)
      kgInfoFinder     <- IOKGInfoFinder(timeRecorder, logger)
    } yield new UpdatesCreatorImpl(gitLabInfoFinder, kgInfoFinder, new UpdatesQueryCreator(renkuBaseUrl))
}
