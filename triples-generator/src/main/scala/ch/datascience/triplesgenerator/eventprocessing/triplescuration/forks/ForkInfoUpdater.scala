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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private[triplescuration] trait ForkInfoUpdater[Interpretation[_]] {
  def updateForkInfo(
      commit:                  Commit,
      givenCuratedTriples:     CuratedTriples
  )(implicit maybeAccessToken: Option[AccessToken]): Interpretation[CuratedTriples]
}

private[triplescuration] class IOForkInfoUpdater(
    gitLab:         GitLabInfoFinder[IO],
    kg:             KGInfoFinder[IO],
    updatesCreator: UpdatesCreator
)(implicit ME:      MonadError[IO, Throwable], cs: ContextShift[IO])
    extends ForkInfoUpdater[IO] {

  import updatesCreator._

  def updateForkInfo(
      commit:                  Commit,
      givenCuratedTriples:     CuratedTriples
  )(implicit maybeAccessToken: Option[AccessToken]): IO[CuratedTriples] =
    (gitLab.findProject(commit.project.path), kg.findProject(commit.project.path)).parMapN {
      case `forks are the same`() => givenCuratedTriples.pure[IO]
      case `forks are different, email and date same`(projectResource, gitLabForkPath) =>
        givenCuratedTriples
          .add(recreateWasDerivedFrom(projectResource, gitLabForkPath))
          .pure[IO]
      case `not only forks are different`(projectResource, gitLabForkPath, gitLabProject) =>
        OptionT
          .fromOption[IO](gitLabProject.maybeEmail)
          .flatMapF(kg.findCreatorId)
          .map { existingUserResource =>
            givenCuratedTriples
              .add(recreateWasDerivedFrom(projectResource, gitLabForkPath))
              .add(swapCreator(projectResource, existingUserResource))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
          .getOrElse {
            givenCuratedTriples
              .add(recreateWasDerivedFrom(projectResource, gitLabForkPath))
              .add(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
      case `no fork in the KG project`(projectResource, gitLabForkPath, gitLabProject) =>
        OptionT
          .fromOption[IO](gitLabProject.maybeEmail)
          .flatMapF(kg.findCreatorId)
          .map { existingUserResource =>
            givenCuratedTriples
              .add(insertWasDerivedFrom(projectResource, gitLabForkPath))
              .add(swapCreator(projectResource, existingUserResource))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
          .getOrElse {
            givenCuratedTriples
              .add(insertWasDerivedFrom(projectResource, gitLabForkPath))
              .add(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
      case `no fork in the GitLab project`(projectResource, gitLabProject) =>
        OptionT
          .fromOption[IO](gitLabProject.maybeEmail)
          .flatMapF(kg.findCreatorId)
          .map { existingUserResource =>
            givenCuratedTriples
              .add(deleteWasDerivedFrom(projectResource))
              .add(swapCreator(projectResource, existingUserResource))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
          .getOrElse {
            givenCuratedTriples
              .add(deleteWasDerivedFrom(projectResource))
              .add(addNewCreator(projectResource, gitLabProject.maybeEmail, gitLabProject.maybeName))
              .add(recreateDateCreated(projectResource, gitLabProject.dateCreated))
          }
      case _ => givenCuratedTriples.pure[IO]
    }.flatten

  private object `forks are the same` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Boolean = tuple match {
      case (Some(gitLabProject), Some(kgProject)) => gitLabProject hasSameForkAs kgProject
      case _                                      => false
    }
  }

  private object `forks are different, email and date same` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, Path)] = tuple match {
      case (Some(gitLabProject), Some(kgProject)) =>
        (kgProject.maybeParentPath -> gitLabProject.maybeParentPath)
          .mapN {
            case (kgFork, gitLabFork)
                if kgFork != gitLabFork && (gitLabProject hasEmailSameAs kgProject) && (gitLabProject hasDateSameAs kgProject) =>
              Option((kgProject.resourceId, gitLabFork))
            case _ => Option.empty[(ResourceId, Path)]
          }
          .getOrElse(Option.empty[(ResourceId, Path)])
      case _ => None
    }
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
  }

  private implicit class GitLabProjectOps(gitLabProject: GitLabProject) {

    lazy val maybeEmail = gitLabProject.maybeCreator.flatMap(_.maybeEmail)
    lazy val maybeName  = gitLabProject.maybeCreator.flatMap(_.maybeName)

    def hasSameForkAs(kgProject: KGProject): Boolean =
      kgProject.maybeParentResourceId.flatMap(_.getPath) == gitLabProject.maybeParentPath

    def hasEmailSameAs(kgProject: KGProject): Boolean =
      (kgProject.creator.maybeEmail -> gitLabProject.maybeCreator.flatMap(_.maybeEmail))
        .mapN { case (kgEmail, gitLabEmail) => kgEmail == gitLabEmail }
        .getOrElse(false)

    def hasDateSameAs(kgProject: KGProject): Boolean =
      kgProject.dateCreated == gitLabProject.dateCreated
  }

  private implicit class ResourceIdOps(resourceId: ResourceId) {
    import scala.util.Try
    lazy val getPath: Option[Path] = resourceId.as[Try, Path].toOption
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
    } yield new IOForkInfoUpdater(gitLabInfoFinder, kgInfoFinder, new UpdatesCreator(renkuBaseUrl))
}
