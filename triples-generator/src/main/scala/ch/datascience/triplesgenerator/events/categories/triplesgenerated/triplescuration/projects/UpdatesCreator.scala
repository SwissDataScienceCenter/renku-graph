/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.MonadError
import cats.data.{EitherT, OptionT}
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.graph.config.{GitLabUrl, RenkuBaseUrl}
import ch.datascience.graph.model.users
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.{SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedEvent
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.IOTriplesCurator.CurationRecoverableError
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait UpdatesCreator[Interpretation[_]] {
  def create(event:     TriplesGeneratedEvent)(implicit
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
      event:                   TriplesGeneratedEvent
  )(implicit maybeAccessToken: Option[AccessToken]): CurationUpdatesGroup[IO] =
    CurationUpdatesGroup[IO](
      "Fork info updates",
      () =>
        EitherT {
          gitLab
            .findProject(event.project.path)
            .flatMap(forkInfoUpdates)
            .map(_.asRight[ProcessingRecoverableError])
            .recover(maybeToRecoverableError)
        }
    )

  private lazy val forkInfoUpdates: Option[GitLabProject] => IO[List[SparqlQuery]] = {
    case `when project has a creator`(creator, gitLabProject) =>
      OptionT(kg.findCreatorId(creator.gitLabId))
        .map(existingUserResource => updateProjectAndSwapCreator(gitLabProject, existingUserResource))
        .getOrElse(updateProjectAndAddCreator(gitLabProject, creator))
    case `when project has no creator`(gitLabProject) =>
      updateProjectAndUnlinkCreator(gitLabProject).pure[IO]
    case _ =>
      List.empty[SparqlQuery].pure[IO]
  }

  private object `when project has a creator` {
    def unapply(maybeProject: Option[GitLabProject]): Option[(GitLabCreator, GitLabProject)] = maybeProject match {
      case Some(project @ GitLabProject(_, _, _, _, _, Some(creator))) => (creator -> project).some
      case _                                                           => None
    }
  }

  private object `when project has no creator` {
    def unapply(maybeProject: Option[GitLabProject]): Option[GitLabProject] = maybeProject match {
      case Some(project @ GitLabProject(_, _, _, _, _, None)) => project.some
      case _                                                  => None
    }
  }

  private def updateProjectAndSwapCreator(gitLabProject: GitLabProject, existingUserResource: users.ResourceId) =
    upsertName(gitLabProject.path, gitLabProject.name) ++
      updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      swapCreator(gitLabProject.path, existingUserResource) ++
      updateDateCreated(gitLabProject.path, gitLabProject.dateCreated) ++
      upsertVisibility(gitLabProject.path, gitLabProject.visibility)

  private def updateProjectAndAddCreator(gitLabProject: GitLabProject, creator: GitLabCreator) =
    upsertName(gitLabProject.path, gitLabProject.name) ++
      updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      addNewCreator(gitLabProject.path, creator) ++
      updateDateCreated(gitLabProject.path, gitLabProject.dateCreated) ++
      upsertVisibility(gitLabProject.path, gitLabProject.visibility)

  private def updateProjectAndUnlinkCreator(gitLabProject: GitLabProject) =
    upsertName(gitLabProject.path, gitLabProject.name) ++
      updateWasDerivedFrom(gitLabProject.path, gitLabProject.maybeParentPath) ++
      unlinkCreator(gitLabProject.path) ++
      updateDateCreated(gitLabProject.path, gitLabProject.dateCreated) ++
      upsertVisibility(gitLabProject.path, gitLabProject.visibility)

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, List[SparqlQuery]]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException) =>
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
