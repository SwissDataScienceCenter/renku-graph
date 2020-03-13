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
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples

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
    (gitLab.findProject(commit.project), kg.findProject(commit.project)) parMapN {
      case `both forks are the same`() => givenCuratedTriples
      case `both forks are different`(projectResource, gitLabForkPath) =>
        givenCuratedTriples
          .add(wasDerivedFromDelete(projectResource))
          .add(wasDerivedFromInsert(projectResource, gitLabForkPath))
      case _ => givenCuratedTriples
    }

  private object `both forks are the same` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Boolean = tuple match {
      case (Some(gitLabProject), Some(kgProject)) => gitLabProject hasSameForkAs kgProject
      case _                                      => false
    }
  }

  private object `both forks are different` {
    def unapply(tuple: (Option[GitLabProject], Option[KGProject])): Option[(ResourceId, Path)] = tuple match {
      case (Some(gitLabProject), Some(kgProject)) =>
        (kgProject.maybeParentResourceId.flatMap(_.getPath) -> gitLabProject.maybeParentPath)
          .mapN {
            case (kgFork, gitLabFork) if kgFork != gitLabFork => Option(kgProject.resourceId -> gitLabFork)
            case _                                            => Option.empty[(ResourceId, Path)]
          }
          .getOrElse(Option.empty[(ResourceId, Path)])
      case _ => None
    }
  }

  private implicit class GitLabProjectOps(gitLabProject: GitLabProject) {

    def hasSameForkAs(kgProject: KGProject): Boolean =
      kgProject.maybeParentResourceId.flatMap(_.getPath) == gitLabProject.maybeParentPath

    def hasOtherForkThan(kgProject: KGProject): Boolean =
      (kgProject.maybeParentResourceId.flatMap(_.getPath) -> gitLabProject.maybeParentPath)
        .mapN { case (kgFork, gitLabFork) => kgFork != gitLabFork }
        .getOrElse(false)
  }

  private implicit class ResourceIdOps(resourceId: ResourceId) {
    import scala.util.Try
    lazy val getPath: Option[Path] = resourceId.as[Try, Path].toOption
  }
}

private[triplescuration] object IOForkInfoUpdater {

  def apply()(implicit cs: ContextShift[IO]): IO[ForkInfoUpdater[IO]] =
    for {
      renkuBaseUrl     <- RenkuBaseUrl[IO]()
      gitLabInfoFinder <- IOGitLabInfoFinder()
      kgInfoFinder     <- IOKGInfoFinder()
    } yield new IOForkInfoUpdater(gitLabInfoFinder, kgInfoFinder, new UpdatesCreator(renkuBaseUrl))
}
