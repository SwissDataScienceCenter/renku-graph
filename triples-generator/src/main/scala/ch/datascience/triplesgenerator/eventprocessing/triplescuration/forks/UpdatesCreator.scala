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
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
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
          (gitLab.findProject(commit.project.path), kg.findProject(commit.project.path))
            .parMapN { forkInfoUpdates }
            .flatten
            .map { _.asRight[ProcessingRecoverableError] }
            .recover(maybeToRecoverableError)
        }
    )

  private lazy val forkInfoUpdates: (Option[GitLabProject], Option[KGProject]) => IO[List[SparqlQuery]] = {
    case `no forks in GitLab and KG`() => List.empty[SparqlQuery].pure[IO]
    case `forks are the same`()        => List.empty[SparqlQuery].pure[IO]
    case `forks are different, creators and dates same`(projectResource, gitLabForkPath) =>
      recreateWasDerivedFrom(projectResource, gitLabForkPath).pure[IO]
    case `not only forks are different`(projectResource, gitLabForkPath, gitLabProject) =>
      OptionT
        .fromOption[IO](gitLabProject.maybeEmail)
        .flatMapF(kg.findCreatorId)
        .map { existingUserResource =>
          recreateWasDerivedFrom(projectResource, gitLabForkPath) ++
            swapCreator(projectResource, existingUserResource) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)
        }
        .getOrElse {
          recreateWasDerivedFrom(projectResource, gitLabForkPath) ++
            addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)
        }
    case `no fork in the KG project`(projectResource, gitLabForkPath, gitLabProject) =>
      OptionT
        .fromOption[IO](gitLabProject.maybeEmail)
        .flatMapF(kg.findCreatorId)
        .map { existingUserResource =>
          insertWasDerivedFrom(projectResource, gitLabForkPath) ++
            swapCreator(projectResource, existingUserResource) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)
        }
        .getOrElse {
          insertWasDerivedFrom(projectResource, gitLabForkPath) ++
            addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)
        }
    case `no fork in the GitLab project`(projectResource, gitLabProject) =>
      OptionT
        .fromOption[IO](gitLabProject.maybeEmail)
        .flatMapF(kg.findCreatorId)
        .map { existingUserResource =>
          deleteWasDerivedFrom(projectResource) ++
            swapCreator(projectResource, existingUserResource) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)

        }
        .getOrElse {
          deleteWasDerivedFrom(projectResource) ++
            addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName) ++
            recreateDateCreated(projectResource, gitLabProject.dateCreated)
        }
    case _ => List.empty[SparqlQuery].pure[IO]
  }

  private object `no forks in GitLab and KG` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Boolean = tuple match {
      case (Some(gitLabProject), Some(kgProject)) =>
        kgProject.maybeParentResourceId.isEmpty && gitLabProject.maybeParentPath.isEmpty
      case _ => false
    }
  }

  private object `forks are the same` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Boolean = tuple match {
      case (Some(gitLabProject), Some(kgProject)) => gitLabProject hasSameForkAs kgProject
      case _                                      => false
    }
  }

  private object `forks are different, creators and dates same` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, Path)] = tuple match {
      case (Some(gitLabProject), Some(kgProject)) =>
        (kgProject.maybeParentPath -> gitLabProject.maybeParentPath)
          .mapN {
            case (kgFork, gitLabFork) if kgFork != gitLabFork && `users and dates are same`(gitLabProject, kgProject) =>
              Option((kgProject.resourceId, gitLabFork))
            case _ => Option.empty[(ResourceId, Path)]
          }
          .getOrElse(Option.empty[(ResourceId, Path)])
      case _ => None
    }

    private def `users and dates are same`(gitLabProject: GitLabProject, kgProject: KGProject): Boolean =
      (gitLabProject hasDateSameAs kgProject) &&
        ((gitLabProject hasEmailSameAs kgProject) || ((gitLabProject hasNoEmailAs kgProject) && (gitLabProject hasUserNameSameAs kgProject)))
  }

  private object `not only forks are different` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, Path, GitLabProject)] =
      tuple match {
        case (Some(gitLabProject), Some(kgProject)) =>
          (kgProject.maybeParentPath, gitLabProject.maybeParentPath)
            .mapN {
              case (kgFork, gitLabFork) if kgFork != gitLabFork =>
                Option((kgProject.resourceId, gitLabFork, gitLabProject))
              case _ => Option.empty[(ResourceId, Path, GitLabProject)]
            }
            .getOrElse(Option.empty[(ResourceId, Path, GitLabProject)])
        case _ => None
      }
  }

  private object `no fork in the KG project` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, Path, GitLabProject)] =
      tuple match {
        case (Some(gitLabProject), Some(kgProject)) if kgProject.maybeParentResourceId.isEmpty =>
          gitLabProject.maybeParentPath map (gitLabFork => (kgProject.resourceId, gitLabFork, gitLabProject))
        case _ => None
      }
  }

  private object `no fork in the GitLab project` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, GitLabProject)] =
      tuple match {
        case (Some(gitLabProject), Some(kgProject)) if gitLabProject.maybeParentPath.isEmpty =>
          Option((kgProject.resourceId, gitLabProject))
        case _ => None
      }
  }

  private implicit class KGProjectOps(kgProject: KGProject) {
    lazy val maybeParentPath: Option[Path] = kgProject.maybeParentResourceId.flatMap(_.getPath)
    lazy val maybeEmail = kgProject.maybeCreator.flatMap(_.maybeEmail)
    lazy val maybeName  = kgProject.maybeCreator.map(_.name)
  }

  private implicit class GitLabProjectOps(gitLabProject: GitLabProject) {

    lazy val maybeEmail = gitLabProject.maybeCreator.flatMap(_.maybeEmail)
    lazy val maybeName  = gitLabProject.maybeCreator.flatMap(_.maybeName)

    def hasSameForkAs(kgProject: KGProject): Boolean =
      (kgProject.maybeParentResourceId.flatMap(_.getPath), gitLabProject.maybeParentPath)
        .mapN(_ == _)
        .getOrElse(false)

    def hasEmailSameAs(kgProject: KGProject): Boolean =
      (kgProject.maybeEmail -> gitLabProject.maybeEmail)
        .mapN { case (kgEmail, gitLabEmail) => kgEmail == gitLabEmail }
        .getOrElse(false)

    def hasNoEmailAs(kgProject: KGProject): Boolean =
      gitLabProject.maybeEmail.isEmpty && kgProject.maybeEmail.isEmpty

    def hasUserNameSameAs(kgProject: KGProject): Boolean =
      (kgProject.maybeName -> gitLabProject.maybeName)
        .mapN { case (kgName, gitLabName) => kgName == gitLabName }
        .getOrElse(false)

    def hasDateSameAs(kgProject: KGProject): Boolean =
      kgProject.dateCreated == gitLabProject.dateCreated
  }

  private implicit class ResourceIdOps(resourceId: ResourceId) {
    import scala.util.Try
    lazy val getPath: Option[Path] = resourceId.as[Try, Path].toOption
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
