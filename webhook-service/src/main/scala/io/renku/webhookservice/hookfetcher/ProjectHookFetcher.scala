/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.webhookservice.hookfetcher

import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.circe.Decoder.decodeList
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.ProjectHookUrl
import org.typelevel.log4cats.Logger

private[webhookservice] trait ProjectHookFetcher[F[_]] {
  def fetchProjectHooks(
      projectId:   projects.Id,
      accessToken: AccessToken
  ): F[List[HookIdAndUrl]]
}

private[webhookservice] object ProjectHookFetcher {
  def apply[F[_]: Async: Logger](gitlabThrottler: Throttler[F, GitLab]) = for {
    gitLabUrl <- GitLabUrlLoader[F]()
  } yield new ProjectHookFetcherImpl[F](gitLabUrl, gitlabThrottler)

  final case class HookIdAndUrl(id: String, url: ProjectHookUrl)
}

private[webhookservice] class ProjectHookFetcherImpl[F[_]: Async: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with ProjectHookFetcher[F] {

  import io.circe._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._
  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[List[HookIdAndUrl]]] = {
    case (Ok, _, response)    => response.as[List[HookIdAndUrl]]
    case (NotFound, _, _)     => List.empty[HookIdAndUrl].pure[F]
    case (Unauthorized, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }

  override def fetchProjectHooks(projectId: projects.Id, accessToken: AccessToken): F[List[HookIdAndUrl]] =
    for {
      uri                <- validateUri(s"$gitLabUrl/api/v4/projects/$projectId/hooks")
      existingHooksNames <- send(request(GET, uri, accessToken))(mapResponse)
    } yield existingHooksNames

  private implicit lazy val hooksIdsAndUrlsDecoder: EntityDecoder[F, List[HookIdAndUrl]] = {
    implicit val hookIdAndUrlDecoder: Decoder[List[HookIdAndUrl]] = decodeList { cursor =>
      for {
        url <- cursor.downField("url").as[String].map(ProjectHookUrl.fromGitlab)
        id  <- cursor.downField("id").as[String] orElse cursor.downField("id").as[Long].map(_.toString)
      } yield HookIdAndUrl(id, url)
    }

    jsonOf[F, List[HookIdAndUrl]]
  }

}
