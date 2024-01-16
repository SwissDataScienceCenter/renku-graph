/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.persons.{GitLabId, Name}
import io.renku.graph.model.projects.Slug
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.projectauth.ProjectMember
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait GLProjectMembersFinder[F[_]] {
  def findProjectMembers(slug: Slug)(implicit at: AccessToken): F[Set[GitLabProjectMember]]
}

private class GLProjectMembersFinderImpl[F[_]: Async: GitLabClient: Logger] extends GLProjectMembersFinder[F] {

  override def findProjectMembers(slug: Slug)(implicit at: AccessToken): F[Set[GitLabProjectMember]] =
    fetch(uri"projects" / slug / "members" / "all")

  private val glApiName: String Refined NonEmpty = "project-members"

  private def fetch(
      uri:        Uri,
      maybePage:  Option[Int] = None,
      allMembers: Set[GitLabProjectMember] = Set.empty
  )(implicit at: AccessToken): F[Set[GitLabProjectMember]] = for {
    uri                     <- addPageToUrl(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- GitLabClient[F].get(uri, glApiName)(mapResponse)(at.some)
    allResults              <- addNextPage(uri, allMembers, fetchedUsersAndNextPage)
  } yield allResults

  private def addPageToUrl(uri: Uri, maybePage: Option[Int] = None) = maybePage match {
    case Some(page) => uri.withQueryParam("page", page.toString)
    case None       => uri
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Set[GitLabProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      response
        .as[List[Option[GitLabProjectMember]]]
        .map(members => members.flatten.toSet -> maybeNextPage(response))
    case (NotFound, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
    case (Unauthorized | Forbidden, _, _) =>
      (Set.empty[GitLabProjectMember] -> Option.empty[Int]).pure[F]
  }

  private def addNextPage(
      uri:                          Uri,
      allUsers:                     Set[GitLabProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabProjectMember], Option[Int])
  )(implicit at: AccessToken): F[Set[GitLabProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(uri, maybeNextPage, allUsers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allUsers ++ fetchedUsers).pure[F]
    }

  private def maybeNextPage(response: Response[F]): Option[Int] =
    response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)

  private implicit val memberDecoder: Decoder[Option[GitLabProjectMember]] = cur =>
    (cur.downField("id").success, cur.downField("name").success)
      .mapN((_, _) => Decoder.forProduct3("id", "name", "access_level")(GitLabProjectMember.apply).map(_.some)(cur))
      .getOrElse(Option.empty[GitLabProjectMember].asRight[DecodingFailure])
}

private object GLProjectMembersFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: GLProjectMembersFinder[F] = new GLProjectMembersFinderImpl[F]
}

private final case class GitLabProjectMember(gitLabId: GitLabId, name: Name, accessLevel: Int) {
  def toProjectAuthMember: ProjectMember =
    ProjectMember.fromGitLabData(gitLabId, accessLevel)
}
