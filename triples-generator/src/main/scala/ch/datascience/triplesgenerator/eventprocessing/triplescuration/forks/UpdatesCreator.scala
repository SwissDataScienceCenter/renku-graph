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
import cats.data.EitherT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.graph.Schemas.schema
import ch.datascience.graph.config.{GitLabUrl, RenkuBaseUrl}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.{Email, Name}
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

private trait UpdatesCreator[Interpretation[_]] {
  def create(commit:    CommitEvent, curatedTriples: CuratedTriples[Interpretation])(implicit
      maybeAccessToken: Option[AccessToken]
  ): CurationUpdatesGroup[Interpretation]
}

private class UpdatesCreatorImpl(
    gitLab:              GitLabInfoFinder[IO],
    kg:                  KGInfoFinder[IO],
    updatesQueryCreator: UpdatesQueryCreator
)(implicit ME:           MonadError[IO, Throwable], cs: ContextShift[IO])
    extends UpdatesCreator[IO] {

  import updatesQueryCreator._

  override def create(
      commit:                  CommitEvent,
      curatedTriples:          CuratedTriples[IO]
  )(implicit maybeAccessToken: Option[AccessToken]): CurationUpdatesGroup[IO] =
    CurationUpdatesGroup[IO](
      "Fork info updates",
      () =>
        EitherT {
          gitLab
            .findProject(commit.project.path)
            .flatMap(forkInfoUpdates(curatedTriples))
            .map(_.asRight[ProcessingRecoverableError])
            .recover(maybeToRecoverableError)
        }
    )

  private def forkInfoUpdates(curatedTriples: CuratedTriples[IO]): Option[GitLabProject] => IO[List[SparqlQuery]] = {
    case `when project has a creator`(creator, gitLabProject) =>
      val (maybeName, maybeEmail) = CreatorInfoExtratorImpl.extract(curatedTriples.triples)
      kg.findCreatorId(creator.gitLabId).map {
        case Some(existingUserResource) => updateProjectAndSwapCreator(gitLabProject, existingUserResource)
        case None                       => updateProjectAndAddCreator(gitLabProject, creator, maybeName, maybeEmail)
      }
    case `when project has no creator`(gitLabProject) =>
      updateProjectAndUnlinkCreator(gitLabProject).pure[IO]
    case _ =>
      List.empty[SparqlQuery].pure[IO]
  }

  private object `when project has a creator` {
    def unapply(maybeProject: Option[GitLabProject]): Option[(GitLabCreator, GitLabProject)] = maybeProject match {
      case Some(project @ GitLabProject(_, _, Some(creator), _)) => (creator -> project).some
      case _                                                     => None
    }
  }

  private object `when project has no creator` {
    def unapply(maybeProject: Option[GitLabProject]): Option[GitLabProject] = maybeProject match {
      case Some(project @ GitLabProject(_, _, None, _)) => project.some
      case _                                            => None
    }
  }

  private def updateProjectAndSwapCreator(gitLabProject: GitLabProject, existingUserResource: users.ResourceId) =
    updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      swapCreator(gitLabProject.path, existingUserResource) ++
      recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)

  private def updateProjectAndAddCreator(gitLabProject:           GitLabProject,
                                         creator:                 GitLabCreator,
                                         maybeCurrentCreatorName: Option[Name],
                                         maybeEmail:              Option[Email]
  ) =
    updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      addNewCreator(gitLabProject.path, creator, maybeCurrentCreatorName, maybeEmail) ++
      recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)

  private def updateProjectAndUnlinkCreator(gitLabProject: GitLabProject) =
    updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      unlinkCreator(gitLabProject.path) ++
      recreateDateCreated(gitLabProject.path, gitLabProject.dateCreated)

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

private object IOUpdateFunctionsCreator {
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
      gitLabApiUrl     <- GitLabUrl[IO]().map(_.apiV4)
      gitLabInfoFinder <- IOGitLabInfoFinder(gitLabThrottler, logger)
      kgInfoFinder     <- IOKGInfoFinder(timeRecorder, logger)
    } yield new UpdatesCreatorImpl(gitLabInfoFinder, kgInfoFinder, new UpdatesQueryCreator(renkuBaseUrl, gitLabApiUrl))
}
