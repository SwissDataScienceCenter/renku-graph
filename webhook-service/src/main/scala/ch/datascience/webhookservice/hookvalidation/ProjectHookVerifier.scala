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

package ch.datascience.webhookservice.hookvalidation

import ProjectHookVerifier.HookIdentifier
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.http.client.{AccessToken, IORestClient}
import ch.datascience.webhookservice.config.GitLab
import ch.datascience.webhookservice.project.ProjectHookUrl
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectHookVerifier[Interpretation[_]] {
  def checkHookPresence(
      projectHookId: HookIdentifier,
      accessToken:   AccessToken
  ): Interpretation[Boolean]
}

private object ProjectHookVerifier {

  final case class HookIdentifier(
      projectId:      ProjectId,
      projectHookUrl: ProjectHookUrl
  )
}

private class IOProjectHookVerifier(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with ProjectHookVerifier[IO] {

  import cats.effect._
  import ch.datascience.http.client.RestClientError.UnauthorizedException
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  override def checkHookPresence(projectHookId: HookIdentifier, accessToken: AccessToken): IO[Boolean] =
    for {
      uri                <- validateUri(s"$gitLabUrl/api/v4/projects/${projectHookId.projectId}/hooks")
      existingHooksNames <- send(request(GET, uri, accessToken))(mapResponse)
    } yield checkProjectHookExists(existingHooksNames, projectHookId.projectHookUrl)

  private lazy val mapResponse: PartialFunction[(Status, Request[IO], Response[IO]), IO[List[String]]] = {
    case (Ok, _, response)    => response.as[List[String]]
    case (Unauthorized, _, _) => IO.raiseError(UnauthorizedException)
  }

  private implicit lazy val hooksUrlsDecoder: EntityDecoder[IO, List[String]] = {
    implicit val hookNameDecoder: Decoder[List[String]] = decodeList {
      _.downField("url").as[String]
    }

    jsonOf[IO, List[String]]
  }

  private def checkProjectHookExists(hooksNames: List[String], urlToFind: ProjectHookUrl): Boolean =
    hooksNames contains urlToFind.value
}
