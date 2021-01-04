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

package ch.datascience.knowledgegraph.projects.rest

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder._
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.{KGProject, Parent}

import scala.concurrent.ExecutionContext
import scala.util.Try

trait ProjectFinder[Interpretation[_]] {
  def findProject(path: Path): Interpretation[Option[Project]]
}

class IOProjectFinder(
    kgProjectFinder:     KGProjectFinder[IO],
    gitLabProjectFinder: GitLabProjectFinder[IO],
    accessTokenFinder:   AccessTokenFinder[IO]
)(implicit ME:           MonadError[IO, Throwable], cs: ContextShift[IO])
    extends ProjectFinder[IO] {

  import accessTokenFinder._
  import gitLabProjectFinder.{findProject => findProjectInGitLab}
  import kgProjectFinder.{findProject => findInKG}

  def findProject(path: Path): IO[Option[Project]] =
    ((OptionT(findInKG(path)), findInGitLab(path)) parMapN (merge(path, _, _))).value

  private def findInGitLab(path: Path) = for {
    accessToken   <- OptionT(findAccessToken(path))
    gitLabProject <- findProjectInGitLab(path, Some(accessToken))
  } yield gitLabProject

  private def merge(path: Path, kgProject: KGProject, gitLabProject: GitLabProject) =
    Project(
      id = gitLabProject.id,
      path = path,
      name = kgProject.name,
      maybeDescription = gitLabProject.maybeDescription,
      visibility = gitLabProject.visibility,
      created = Creation(
        date = kgProject.created.date,
        maybeCreator = kgProject.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
      ),
      updatedAt = gitLabProject.updatedAt,
      urls = gitLabProject.urls,
      forking = Forking(gitLabProject.forksCount, kgProject.maybeParent.toParentProject),
      tags = gitLabProject.tags,
      starsCount = gitLabProject.starsCount,
      permissions = gitLabProject.permissions,
      statistics = gitLabProject.statistics,
      version = kgProject.version
    )

  private implicit class ParentOps(maybeParent: Option[Parent]) {
    lazy val toParentProject: Option[ParentProject] =
      (maybeParent -> maybeParent.flatMap(_.resourceId.as[Try, Path].toOption)) mapN { case (parent, path) =>
        ParentProject(
          path,
          parent.name,
          Creation(parent.created.date,
                   parent.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
          )
        )
      }
  }
}

private object IOProjectFinder {
  import cats.effect.{ContextShift, IO, Timer}
  import ch.datascience.rdfstore.SparqlQueryTimeRecorder
  import com.typesafe.config.{Config, ConfigFactory}
  import io.chrisdavenport.log4cats.Logger

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO],
      timeRecorder:    SparqlQueryTimeRecorder[IO],
      config:          Config = ConfigFactory.load()
  )(implicit
      ME:               MonadError[IO, Throwable],
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectFinder[IO]] =
    for {
      kgProjectFinder     <- IOKGProjectFinder(timeRecorder, logger = logger)
      gitLabProjectFinder <- IOGitLabProjectFinder(gitLabThrottler, logger)
      accessTokenFinder   <- IOAccessTokenFinder(logger)
    } yield new IOProjectFinder(kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
}
