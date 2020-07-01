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
import ch.datascience.rdfstore.{JsonLDTriples, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private[triplescuration] trait ForkInfoUpdater[Interpretation[_]] {
  def updateForkInfo(
      commit:                  CommitEvent,
      givenCuratedTriples:     CuratedTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[Interpretation]
}

private[triplescuration] class IOForkInfoUpdater(
    gitLab:                   GitLabInfoFinder[IO],
    kg:                       KGInfoFinder[IO],
    updatesCreator:           UpdatesCreator,
    projectPropertiesRemover: JsonLDTriples => JsonLDTriples
)(implicit ME:                MonadError[IO, Throwable], cs: ContextShift[IO])
    extends ForkInfoUpdater[IO] {

  import updatesCreator._

  def updateForkInfo(
      commit:                  CommitEvent,
      givenCuratedTriples:     CuratedTriples
  )(implicit maybeAccessToken: Option[AccessToken]): CurationResults[IO] = EitherT {
    (gitLab.findProject(commit.project.path), kg.findProject(commit.project.path))
      .parMapN {
        case `no forks in GitLab and KG`() => givenCuratedTriples.pure[IO]
        case `forks are the same`() =>
          givenCuratedTriples
            .transformTriples(projectPropertiesRemover)
            .pure[IO]
        case `forks are different, creators and dates same`(projectResource, gitLabForkPath) =>
          givenCuratedTriples
            .transformTriples(projectPropertiesRemover)
            .addUpdates(recreateWasDerivedFrom(projectResource, gitLabForkPath))
            .pure[IO]
        case `not only forks are different`(projectResource, gitLabForkPath, gitLabProject) =>
          OptionT
            .fromOption[IO](gitLabProject.maybeEmail)
            .flatMapF(kg.findCreatorId)
            .map { existingUserResource =>
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(recreateWasDerivedFrom(projectResource, gitLabForkPath))
                .addUpdates(swapCreator(projectResource, existingUserResource))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
            .getOrElse {
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(recreateWasDerivedFrom(projectResource, gitLabForkPath))
                .addUpdates(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
        case `no fork in the KG project`(projectResource, gitLabForkPath, gitLabProject) =>
          OptionT
            .fromOption[IO](gitLabProject.maybeEmail)
            .flatMapF(kg.findCreatorId)
            .map { existingUserResource =>
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(insertWasDerivedFrom(projectResource, gitLabForkPath))
                .addUpdates(swapCreator(projectResource, existingUserResource))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
            .getOrElse {
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(insertWasDerivedFrom(projectResource, gitLabForkPath))
                .addUpdates(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
        case `no fork in the GitLab project`(projectResource, gitLabProject) =>
          OptionT
            .fromOption[IO](gitLabProject.maybeEmail)
            .flatMapF(kg.findCreatorId)
            .map { existingUserResource =>
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(deleteWasDerivedFrom(projectResource))
                .addUpdates(swapCreator(projectResource, existingUserResource))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
            .getOrElse {
              givenCuratedTriples
                .transformTriples(projectPropertiesRemover)
                .addUpdates(deleteWasDerivedFrom(projectResource))
                .addUpdates(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
                .addUpdates(recreateDateCreated(projectResource, gitLabProject.dateCreated))
            }
        case _ => givenCuratedTriples.pure[IO]
      }
      .flatten
      .map(_.asRight[ProcessingRecoverableError])
      .recover(maybeToRecoverableError)
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
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, CuratedTriples]] = {
    case e: UnexpectedResponseException =>
      Left[ProcessingRecoverableError, CuratedTriples](CurationRecoverableError("Problem with finding fork info", e))
    case e: ConnectivityException =>
      Left[ProcessingRecoverableError, CuratedTriples](CurationRecoverableError("Problem with finding fork info", e))
  }
}

private[triplescuration] object IOForkInfoUpdater {
  import cats.effect.Timer
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[ForkInfoUpdater[IO]] =
    for {
      renkuBaseUrl     <- RenkuBaseUrl[IO]()
      gitLabInfoFinder <- IOGitLabInfoFinder(gitLabThrottler, logger)
      kgInfoFinder     <- IOKGInfoFinder(timeRecorder, logger)
      triplesTransformer = new ProjectPropertiesRemover
    } yield new IOForkInfoUpdater(gitLabInfoFinder, kgInfoFinder, new UpdatesCreator(renkuBaseUrl), triplesTransformer)
}
