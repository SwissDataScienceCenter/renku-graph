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
import io.renku.http.rest.paging.model.Page
import io.renku.webhookservice.hookfetcher.ProjectHookFetcher.HookIdAndUrl
import io.renku.webhookservice.model.ProjectHookUrl
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.log4cats.Logger
import org.typelevel.ci._

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
  import fs2.Stream
  import io.renku.http.tinytypes.TinyTypeURIEncoder._
  import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
  import org.http4s._
  import org.http4s.circe.CirceEntityDecoder._

  override def fetchProjectHooks(projectId:   projects.GitLabId,
                                 accessToken: AccessToken
  ): F[Option[List[HookIdAndUrl]]] = {
    def fetchHooks(page: Page) =
      GitLabClient[F].get(uri"projects" / projectId / "hooks" withQueryParam ("page", page), "project-hooks")(
        mapResponse
      )(accessToken.some)

    def readNextPage
        : ((Option[List[HookIdAndUrl]], Option[Page])) => Stream[F, (Option[List[HookIdAndUrl]], Option[Page])] = {
      case (previousHooks, Some(nextPage)) =>
        Stream
          .eval(fetchHooks(nextPage))
          .map { case (currentHooks, maybeNextPage) => (previousHooks |+| currentHooks) -> maybeNextPage }
          .flatMap(readNextPage)
      case (previousHooks, None) =>
        Stream.emit(previousHooks -> None)
    }

    readNextPage(List.empty[HookIdAndUrl].some -> Page.first.some)
      .map { case (maybeHooks, _) => maybeHooks }
      .compile
      .toList
      .map(_.sequence.map(_.flatten))
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Option[List[HookIdAndUrl]], Option[Page])]] = {
    case (Ok, _, response) =>
      val maybeNextPage: Option[Page] =
        response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption.map(Page(_)))
      response.as[List[HookIdAndUrl]].map(_.some -> maybeNextPage)
    case (NotFound, _, _)                 => (List.empty[HookIdAndUrl].some -> Option.empty[Page]).pure[F]
    case (Unauthorized | Forbidden, _, _) => (Option.empty[List[HookIdAndUrl]] -> Option.empty[Page]).pure[F]
  }

  private implicit val decoder: Decoder[List[HookIdAndUrl]] = decodeList { cursor =>
    for {
      url <- cursor.downField("url").as[String].map(ProjectHookUrl.fromGitlab)
      id  <- cursor.downField("id").as[Int]
    } yield HookIdAndUrl(id, url)
  }
}
