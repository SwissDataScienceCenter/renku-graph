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

package io.renku.graph.http.server.security

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeOption
import io.renku.graph.http.server.security.Authorizer.{SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient, UserAccessToken}
import io.renku.http.server.security.model.AuthUser
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import org.http4s.Status.{Forbidden, NotFound, Ok, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.implicits._
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

trait GitLabPathRecordsFinder[F[_]] extends SecurityRecordFinder[F, projects.Path]

object GitLabPathRecordsFinder {
  def apply[F[_]: Async: Parallel: Logger: GitLabClient]: F[GitLabPathRecordsFinder[F]] =
    new GitLabPathRecordsFinderImpl[F](new VisibilityFinderImpl[F], new MembersFinderImpl[F]).pure[F].widen
}

private class GitLabPathRecordsFinderImpl[F[_]: Async: Parallel](visibilityFinder: VisibilityFinder[F],
                                                                 membersFinder: MembersFinder[F]
) extends GitLabPathRecordsFinder[F] {

  import membersFinder.findMembers
  import visibilityFinder.findVisibility

  override def apply(path: projects.Path, maybeAuthUser: Option[AuthUser]): F[List[SecurityRecord]] = {
    implicit val maybeAccessToken: Option[UserAccessToken] = maybeAuthUser.map(_.accessToken)

    (findVisibility(path) -> findMembers(path))
      .parMapN((maybeVisibility, members) => maybeVisibility.map(vis => SecurityRecord(vis, path, members)).toList)
  }
}

private trait VisibilityFinder[F[_]] {
  def findVisibility(path: projects.Path)(implicit mat: Option[AccessToken]): F[Option[projects.Visibility]]
}

private class VisibilityFinderImpl[F[_]: Async: GitLabClient: Logger] extends VisibilityFinder[F] {

  override def findVisibility(path: projects.Path)(implicit mat: Option[AccessToken]): F[Option[projects.Visibility]] =
    GitLabClient[F].get(uri"projects" / path, "single-project")(mapResponse)

  private lazy val mapResponse: PartialFunction[(Status, Request[F], Response[F]), F[Option[projects.Visibility]]] = {
    case (Ok, _, response)                           => response.as[Option[projects.Visibility]]
    case (Unauthorized | Forbidden | NotFound, _, _) => Option.empty[projects.Visibility].pure[F]
  }

  private implicit lazy val entityDecoder: EntityDecoder[F, Option[projects.Visibility]] = {

    implicit val decoder: Decoder[Option[projects.Visibility]] = { cur =>
      cur.downField("visibility").as[Option[projects.Visibility]](decodeOption(projects.Visibility.jsonDecoder))
    }

    jsonOf[F, Option[projects.Visibility]]
  }
}

private trait MembersFinder[F[_]] {
  def findMembers(path: projects.Path)(implicit mat: Option[AccessToken]): F[Set[persons.GitLabId]]
}

private class MembersFinderImpl[F[_]: Async: GitLabClient: Logger] extends MembersFinder[F] {

  override def findMembers(path: projects.Path)(implicit mat: Option[AccessToken]): F[Set[persons.GitLabId]] =
    fetch(uri"projects" / path / "members")

  private def fetch(
      uri:        Uri,
      maybePage:  Option[Int] = None,
      allMembers: Set[persons.GitLabId] = Set.empty
  )(implicit mat: Option[AccessToken]): F[Set[persons.GitLabId]] = for {
    uri                     <- uriWithPage(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- GitLabClient[F].get(uri, "project-members")(mapResponse)
    allMembers              <- addNextPage(uri, allMembers, fetchedUsersAndNextPage)
  } yield allMembers

  private def uriWithPage(uri: Uri, maybePage: Option[Int]) = maybePage match {
    case Some(page) => uri withQueryParam ("page", page)
    case None       => uri
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Set[persons.GitLabId], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage: Option[Int] = response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)
      response.as[List[persons.GitLabId]].map(_.toSet -> maybeNextPage)
    case (Unauthorized | Forbidden | NotFound, _, _) => (Set.empty[persons.GitLabId] -> Option.empty[Int]).pure[F]
  }

  private def addNextPage(
      url:                        Uri,
      allMembers:                 Set[persons.GitLabId],
      fetchedIdsAndMaybeNextPage: (Set[persons.GitLabId], Option[Int])
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[persons.GitLabId]] =
    fetchedIdsAndMaybeNextPage match {
      case (fetchedIds, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allMembers ++ fetchedIds)
      case (fetchedIds, None)                    => (allMembers ++ fetchedIds).pure[F]
    }

  private implicit lazy val entityDecoder: EntityDecoder[F, List[persons.GitLabId]] = {

    implicit val decoder: Decoder[persons.GitLabId] = { cursor =>
      import io.renku.tinytypes.json.TinyTypeDecoders.intDecoder
      cursor.downField("id").as[persons.GitLabId](intDecoder(persons.GitLabId))
    }

    jsonOf[F, List[persons.GitLabId]]
  }
}
