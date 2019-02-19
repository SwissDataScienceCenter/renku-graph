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

package ch.datascience.webhookservice.hookvalidation

import ProjectHookVerifier.HookIdentifier
import cats.effect.IO
import ch.datascience.clients.{AccessToken, IORestClient}
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.IOContextShift
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.project.ProjectHookUrlFinder.ProjectHookUrl
import io.circe.Decoder.decodeList
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait ProjectHookVerifier[Interpretation[_]] {
  def checkProjectHookPresence(
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

@Singleton
private class IOProjectHookVerifier @Inject()(
    gitLabUrlProvider:       IOGitLabConfigProvider
)(implicit executionContext: ExecutionContext, contextShift: IOContextShift)
    extends IORestClient
    with ProjectHookVerifier[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  override def checkProjectHookPresence(projectHookId: HookIdentifier, accessToken: AccessToken): IO[Boolean] =
    for {
      gitLabHostUrl      <- gitLabUrlProvider.get()
      uri                <- validateUri(s"$gitLabHostUrl/api/v4/projects/${projectHookId.projectId}/hooks")
      existingHooksNames <- send(request(GET, uri, accessToken))(mapResponse)
    } yield checkProjectHookExists(existingHooksNames, projectHookId.projectHookUrl)

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[List[String]] =
    response.status match {
      case Ok           => response.as[List[String]] handleErrorWith contextToError(request, response)
      case Unauthorized => IO.raiseError(UnauthorizedException)
      case _            => raiseError(request, response)
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
