/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import IOAccessTokenFinder._
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.config.GitLab
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.KGProject

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait ProjectFinder[Interpretation[_]] {
  def findProject(path: ProjectPath): Interpretation[Option[Project]]
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

  def findProject(path: ProjectPath): IO[Option[Project]] =
    ((OptionT(findInKG(path)), findInGitLab(path)) parMapN (merge(path, _, _))).value

  private def findInGitLab(path: ProjectPath) =
    for {
      accessToken   <- OptionT(findAccessToken(path)) flatTransform noneToError(path)
      gitLabProject <- findProjectInGitLab(path, Some(accessToken))
    } yield gitLabProject

  private def noneToError(path: ProjectPath): Option[AccessToken] => IO[Option[AccessToken]] = {
    case Some(accessToken) => Option(accessToken).pure[IO]
    case None              => raiseError(s"No access token for $path")
  }

  private def raiseError[Out](message: String) = new Exception(message).raiseError[IO, Out]

  private def merge(path: ProjectPath, kgProject: KGProject, gitLabProject: GitLabProject) =
    Project(
      path = path,
      name = kgProject.name,
      created = Creation(
        date    = kgProject.created.date,
        creator = Creator(kgProject.created.creator.email, kgProject.created.creator.name)
      ),
      repoUrls = RepoUrls(gitLabProject.urls.ssh, gitLabProject.urls.http)
    )
}

private object IOProjectFinder {
  import cats.effect.{ContextShift, IO, Timer}
  import com.typesafe.config.{Config, ConfigFactory}
  import io.chrisdavenport.log4cats.Logger

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO],
      config:          Config = ConfigFactory.load()
  )(implicit ME:       MonadError[IO, Throwable],
    executionContext:  ExecutionContext,
    contextShift:      ContextShift[IO],
    timer:             Timer[IO]): IO[ProjectFinder[IO]] =
    for {
      kgProjectFinder     <- IOKGProjectFinder(logger = logger)
      gitLabProjectFinder <- IOGitLabProjectFinder(gitLabThrottler, logger)
      accessTokenFinder   <- IOAccessTokenFinder(logger)
    } yield new IOProjectFinder(kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
}
