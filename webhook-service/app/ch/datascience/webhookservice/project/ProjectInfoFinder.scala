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

package ch.datascience.webhookservice.project

import cats.effect.IO
import ch.datascience.clients.{AccessToken, IORestClient}
import ch.datascience.graph.events._
import ch.datascience.webhookservice.config.IOGitLabConfigProvider
import ch.datascience.webhookservice.model.{ProjectInfo, ProjectOwner, ProjectVisibility}
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait ProjectInfoFinder[Interpretation[_]] {
  def findProjectInfo(
      projectId:   ProjectId,
      accessToken: AccessToken
  ): Interpretation[ProjectInfo]
}

@Singleton
class IOProjectInfoFinder @Inject()(gitLabConfigProvider: IOGitLabConfigProvider)(
    implicit executionContext:                            ExecutionContext)
    extends IORestClient
    with ProjectInfoFinder[IO] {

  import cats.effect._
  import ch.datascience.webhookservice.exceptions.UnauthorizedException
  import io.circe._
  import org.http4s.Method.GET
  import org.http4s.Status.Unauthorized
  import org.http4s._
  import org.http4s.circe._
  import org.http4s.dsl.io._

  def findProjectInfo(projectId: ProjectId, accessToken: AccessToken): IO[ProjectInfo] =
    for {
      gitLabHostUrl <- gitLabConfigProvider.get()
      uri           <- validateUri(s"$gitLabHostUrl/api/v4/projects/$projectId")
      projectInfo   <- send(request(GET, uri, accessToken))(mapResponse)
    } yield projectInfo

  private def mapResponse(request: Request[IO], response: Response[IO]): IO[ProjectInfo] =
    response.status match {
      case Ok           => response.as[ProjectInfo] handleErrorWith contextToError(request, response)
      case Unauthorized => F.raiseError(UnauthorizedException)
      case _            => raiseError(request, response)
    }

  private implicit lazy val projectInfoDecoder: EntityDecoder[IO, ProjectInfo] = {
    implicit val hookNameDecoder: Decoder[ProjectInfo] = (cursor: HCursor) =>
      for {
        id         <- cursor.downField("id").as[ProjectId]
        visibility <- cursor.downField("visibility").as[ProjectVisibility]
        path       <- cursor.downField("path_with_namespace").as[ProjectPath]
        ownerId    <- cursor.downField("owner").downField("id").as[UserId]
      } yield ProjectInfo(id, visibility, path, ProjectOwner(ownerId))

    jsonOf[IO, ProjectInfo]
  }
}
