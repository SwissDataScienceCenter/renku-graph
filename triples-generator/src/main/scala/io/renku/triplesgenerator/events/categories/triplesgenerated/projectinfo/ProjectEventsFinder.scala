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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.RestClient.ResponseMappingF
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.http4s.EntityDecoder
import org.http4s.Method.GET
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait ProjectEventsFinder[F[_]] {
  def find(project:     Project, page: Int)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, (List[PushEvent], PagingInfo)]
}

private class ProjectEventsFinderImpl[F[_]: Async: Logger](
    gitLabClient:     GitLabClient[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectEventsFinder[F] {

  override def find(project: Project, page: Int)(implicit
      maybeAccessToken:      Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, (List[PushEvent], PagingInfo)] =
    EitherT {
      gitLabClient
        .send(
          GET,
          uri"projects" / project.id.show / "events" withQueryParams Map("action" -> "pushed", "page" -> page.toString),
          "project-events"
        )(mapResponse)
        .map(_.asRight[ProcessingRecoverableError])
        .recoverWith(recoveryStrategy.maybeRecoverableError)
    }

  private lazy val mapResponse: ResponseMappingF[F, (List[PushEvent], PagingInfo)] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage   = response.headers.get(ci"X-Next-Page") >>= (_.head.value.toIntOption)
      lazy val maybeTotalPages = response.headers.get(ci"X-Total-Pages") >>= (_.head.value.toIntOption)
      response.as[List[PushEvent]].map(_ -> PagingInfo(maybeNextPage, maybeTotalPages))
    case (NotFound, _, _) => (List.empty[PushEvent] -> PagingInfo(maybeNextPage = None, maybeTotalPages = None)).pure[F]
  }

  private implicit lazy val eventsDecoder: EntityDecoder[F, List[PushEvent]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import org.http4s.circe.jsonOf

    implicit val events: Decoder[Option[PushEvent]] = cursor =>
      for {
        projectId  <- cursor.downField("project_id").as[projects.Id]
        commitFrom <- cursor.downField("push_data").downField("commit_from").as[Option[CommitId]]
        commitTo   <- cursor.downField("push_data").downField("commit_to").as[Option[CommitId]]
        authorId   <- cursor.downField("author").downField("id").as[persons.GitLabId]
        authorName <- cursor.downField("author").downField("name").as[persons.Name]
      } yield (commitTo orElse commitFrom).map(PushEvent(projectId, _, authorId, authorName))

    jsonOf[F, List[Option[PushEvent]]].map(_.flatten)
  }
}

private object ProjectEventsFinder {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[ProjectEventsFinder[F]] =
    new ProjectEventsFinderImpl[F](gitLabClient).pure[F].widen[ProjectEventsFinder[F]]
}

private case class PushEvent(projectId:  projects.Id,
                             commitId:   CommitId,
                             authorId:   persons.GitLabId,
                             authorName: persons.Name
)

private case class PagingInfo(maybeNextPage: Option[Int], maybeTotalPages: Option[Int]) {

  private val pagesToCheck = 30
  private val step         = maybeTotalPages.map(_ / pagesToCheck).getOrElse(1)

  lazy val findNextPage: Option[Int] =
    if (maybeTotalPages.isEmpty) maybeNextPage
    else
      (maybeNextPage -> maybeTotalPages) mapN {
        case nextPage -> total if total < pagesToCheck => nextPage
        case nextPage -> total if nextPage == total    => nextPage
        case nextPage -> _ if nextPage     % step == 0 => nextPage
        case nextPage -> total if nextPage % step > 0 =>
          val next = nextPage - (nextPage % step) + step
          if (next >= total) total
          else next
      }
}
