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

package io.renku.tokenrepository.repository.association

import cats.effect.Async
import cats.effect.kernel.Temporal
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.{RateLimit, Throttler}
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.circe.jsonOf
import org.typelevel.log4cats.Logger

trait ProjectPathFinder[F[_]] {
  def findProjectPath(projectId: projects.Id, maybeAccessToken: Option[AccessToken]): F[Option[projects.Path]]
}

private class ProjectPathFinderImpl[F[_]: Async: Temporal: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with ProjectPathFinder[F] {

  import cats.effect._
  import cats.syntax.all._
  import io.circe._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.dsl.io._

  def findProjectPath(projectId: projects.Id, maybeAccessToken: Option[AccessToken]): F[Option[projects.Path]] =
    for {
      uri     <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId")
      project <- send(request(GET, uri, maybeAccessToken))(mapResponse)
    } yield project

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.Path]]] = {
    case (Ok, _, response)    => response.as[projects.Path].map(Option.apply)
    case (NotFound, _, _)     => Option.empty[projects.Path].pure[F]
    case (Unauthorized, _, _) => Option.empty[projects.Path].pure[F]
  }

  private implicit lazy val projectPathDecoder: EntityDecoder[F, projects.Path] = {
    lazy val decoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]
    jsonOf[F, projects.Path](Sync[F], decoder)
  }
}

object ProjectPathFinder {

  def apply[F[_]: Async: Temporal: Logger]: F[ProjectPathFinder[F]] = for {
    gitLabRateLimit <- RateLimit.fromConfig[F, GitLab]("services.gitlab.rate-limit")
    gitLabThrottler <- Throttler[F, GitLab](gitLabRateLimit)
    gitLabUrl       <- GitLabUrlLoader[F]()
  } yield new ProjectPathFinderImpl(gitLabUrl, gitLabThrottler)
}
