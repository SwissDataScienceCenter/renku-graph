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

package io.renku.webhookservice.hookvalidation

import cats.Applicative
import cats.effect.{Async, MonadCancelThrow}
import cats.syntax.all._
import io.circe.Decoder.decodeList
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.projects.Id
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.webhookservice.hookvalidation.ProjectHookVerifier.HookIdentifier
import io.renku.webhookservice.model.ProjectHookUrl
import org.typelevel.log4cats.Logger

private trait ProjectHookVerifier[Interpretation[_]] {
  def checkHookPresence(
      projectHookId: HookIdentifier,
      accessToken:   AccessToken
  ): Interpretation[Boolean]
}

private object ProjectHookVerifier {
  final case class HookIdentifier(projectId: Id, projectHookUrl: ProjectHookUrl)

  def apply[F[_]: Async: Logger](gitLabUrl: GitLabUrl, gitlabThrottler: Throttler[F, GitLab]) =
    Applicative[F].pure(new ProjectHookVerifierImpl[F](gitLabUrl, gitlabThrottler))
}

private class ProjectHookVerifierImpl[F[_]: Async: Logger](
    gitLabUrl:       GitLabUrl,
    gitLabThrottler: Throttler[F, GitLab]
) extends RestClient(gitLabThrottler)
    with ProjectHookVerifier[F] {

  import io.circe._
  import io.renku.http.client.RestClientError.UnauthorizedException
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  override def checkHookPresence(projectHookId: HookIdentifier, accessToken: AccessToken): F[Boolean] =
    for {
      uri                <- validateUri(s"$gitLabUrl/api/v4/projects/${projectHookId.projectId}/hooks")
      existingHooksNames <- send(request(GET, uri, accessToken))(mapResponse)
    } yield checkProjectHookExists(existingHooksNames, projectHookId.projectHookUrl)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[List[String]]] = {
    case (Ok, _, response)    => response.as[List[String]]
    case (Unauthorized, _, _) => MonadCancelThrow[F].raiseError(UnauthorizedException)
  }

  private implicit lazy val hooksUrlsDecoder: EntityDecoder[F, List[String]] = {
    implicit val hookNameDecoder: Decoder[List[String]] = decodeList {
      _.downField("url").as[String]
    }

    jsonOf[F, List[String]]
  }

  private def checkProjectHookExists(hooksNames: List[String], urlToFind: ProjectHookUrl): Boolean =
    hooksNames contains urlToFind.value
}
