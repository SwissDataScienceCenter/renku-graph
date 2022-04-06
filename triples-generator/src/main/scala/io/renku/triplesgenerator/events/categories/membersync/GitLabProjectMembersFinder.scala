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

package io.renku.triplesgenerator.events.categories.membersync

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder
import io.renku.graph.model.persons.{GitLabId, Name}
import io.renku.graph.model.projects.Path
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Method.GET
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait GitLabProjectMembersFinder[F[_]] {
  def findProjectMembers(path: Path)(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]]
}

private class GitLabProjectMembersFinderImpl[F[_]: Async: Logger](gitLabClient: GitLabClient[F])
    extends GitLabProjectMembersFinder[F] {

  override def findProjectMembers(
      path:                    Path
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] =
    for {
      users   <- fetch(uri"projects" / urlEncode(path.value) / "users", "users")
      members <- fetch(uri"projects" / urlEncode(path.value) / "members", "members")
    } yield users ++ members

  private def fetch(
      uri:                     Uri,
      endpointName:            NonEmptyString,
      maybePage:               Option[Int] = None,
      allUsers:                Set[GitLabProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabProjectMember]] = for {
    uri                     <- addPageToUrl(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- gitLabClient.send(GET, uri, endpointName)(mapResponse(uri, endpointName))
    allUsers                <- addNextPage(uri, endpointName, allUsers, fetchedUsersAndNextPage)
  } yield allUsers

  private def addPageToUrl(uri: Uri, maybePage: Option[Int] = None) = maybePage match {
    case Some(page) => uri.withQueryParam("page", page.toString)
    case None       => uri
  }

  private def mapResponse(uri: Uri, endpointName: NonEmptyString)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): PartialFunction[(Status, Request[F], Response[F]), F[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[GitLabProjectMember]]
        .map(members => members.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      maybeAccessToken match {
        case Some(_) => fetch(uri, endpointName)(maybeAccessToken = None).map(_ -> None)
        case None    => (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
      }
  }

  private def addNextPage(
      uri:                          Uri,
      endpointName:                 NonEmptyString,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): F[Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(uri, endpointName, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[F]
    }

  private def maybeNextPage(response: Response[F]): Option[Int] =
    response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)

  private implicit lazy val projectDecoder: EntityDecoder[F, List[GitLabProjectMember]] = {
    import io.renku.graph.model.persons

    implicit val decoder: Decoder[GitLabProjectMember] = { cursor =>
      for {
        id   <- cursor.downField("id").as[GitLabId]
        name <- cursor.downField("name").as[persons.Name]
      } yield GitLabProjectMember(id, name)
    }

    jsonOf[F, List[GitLabProjectMember]]
  }

  type NonEmptyString = String Refined NonEmpty
}

private object GitLabProjectMembersFinder {

  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[GitLabProjectMembersFinder[F]] =
    new GitLabProjectMembersFinderImpl[F](gitLabClient).pure[F].widen[GitLabProjectMembersFinder[F]]
}

private final case class GitLabProjectMember(gitLabId: GitLabId, name: Name)
