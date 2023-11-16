/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder.decodeList
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger

private[webhookservice] trait ProjectHookFetcher[F[_]] {
  def fetchProjectHooks(projectId: projects.GitLabId, accessToken: AccessToken): F[Option[List[HookIdAndUrl]]]
}

private[webhookservice] object ProjectHookFetcher {
  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectHookFetcher[F]] =
    new ProjectHookFetcherImpl[F].pure[F].widen

  final case class HookIdAndUrl(id: Int, url: ProjectHookUrl)
}

private[webhookservice] class ProjectHookFetcherImpl[F[_]: Async: GitLabClient: Logger] extends ProjectHookFetcher[F] {

  import io.circe._
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
  import org.http4s._
  import org.http4s.circe.CirceEntityDecoder._

  override def fetchProjectHooks(projectId:   projects.GitLabId,
                                 accessToken: AccessToken
  ): F[Option[List[HookIdAndUrl]]] =
    GitLabClient[F].get(uri"projects" / projectId / "hooks", "project-hooks")(mapResponse)(accessToken.some)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[List[HookIdAndUrl]]]] = {
    case (Ok, _, response)                => response.as[List[HookIdAndUrl]].map(_.some)
    case (NotFound, _, _)                 => List.empty[HookIdAndUrl].some.pure[F]
    case (Unauthorized | Forbidden, _, _) => Option.empty[List[HookIdAndUrl]].pure[F]
  }

  private implicit val decoder: Decoder[List[HookIdAndUrl]] = decodeList { cursor =>
    for {
      url <- cursor.downField("url").as[String].map(ProjectHookUrl.fromGitlab)
      id  <- cursor.downField("id").as[Int]
    } yield HookIdAndUrl(id, url)
  }
}
