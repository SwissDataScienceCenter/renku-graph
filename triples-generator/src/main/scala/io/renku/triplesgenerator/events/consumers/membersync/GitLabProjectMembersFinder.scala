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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder
import io.renku.graph.model.persons.{GitLabId, Name}
import io.renku.graph.model.projects.Path
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait GitLabProjectMembersFinder[F[_]] {
  def findProjectMembers(path: Path)(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]]
}

private class GitLabProjectMembersFinderImpl[F[_]: Async: GitLabClient: Logger] extends GitLabProjectMembersFinder[F] {

  override def findProjectMembers(path: Path)(implicit mat: Option[AccessToken]): F[Set[GitLabProjectMember]] =
    fetch(uri"projects" / path.show / "members" / "all")

  private val glApiName: String Refined NonEmpty = "project-members"

  private def fetch(
      uri:        Uri,
      maybePage:  Option[Int] = None,
      allMembers: Set[GitLabProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] = for {
    uri                     <- addPageToUrl(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- GitLabClient[F].get(uri, glApiName)(mapResponse(uri))
    allResults              <- addNextPage(uri, allMembers, fetchedUsersAndNextPage)
  } yield allResults

  private def addPageToUrl(uri: Uri, maybePage: Option[Int] = None) = maybePage match {
    case Some(page) => uri.withQueryParam("page", page.toString)
    case None       => uri
  }

  private def mapResponse(uri: Uri)(implicit
      maybeAccessToken: Option[AccessToken]
  ): PartialFunction[(Status, Request[F], Response[F]), F[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => members.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      maybeAccessToken match {
        case Some(_) => fetch(uri)(maybeAccessToken = None).map(_ -> None)
        case None    => (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
      }
  }

  private def addNextPage(
      uri:                          Uri,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(uri, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[F]
    }

  private def maybeNextPage(response: Response[F]): Option[Int] =
    response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[F, List[GitLabProjectMember]] = {
    import io.renku.graph.model.persons

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      (cursor.downField("id").as[GitLabId] -> cursor.downField("name").as[persons.Name])
        .mapN(GitLabProjectMember)
    }

    jsonOf[F, List[GitLabProjectMember]]
  }
}

private object GitLabProjectMembersFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[GitLabProjectMembersFinder[F]] =
    new GitLabProjectMembersFinderImpl[F].pure[F].widen[GitLabProjectMembersFinder[F]]
}

private final case class GitLabProjectMember(gitLabId: GitLabId, name: Name)
