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

package io.renku.triplesgenerator.events.consumers.projectinfo

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.graph.model.gitlab.GitLabMember
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.errors.{ProcessingRecoverableError, RecoverableErrorsRecovery}
import org.http4s._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait ProjectMembersFinder[F[_]] {
  def findProjectMembers(slug: projects.Slug)(implicit
      at: AccessToken
  ): EitherT[F, ProcessingRecoverableError, Set[GitLabMember]]
}

private object ProjectMembersFinder {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: Logger]: F[ProjectMembersFinder[F]] =
    new ProjectMembersFinderImpl[F].pure[F].widen[ProjectMembersFinder[F]]
}

private class ProjectMembersFinderImpl[F[_]: Async: NonEmptyParallel: GitLabClient: Logger](
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectMembersFinder[F]
    with GitlabJsonDecoder {

  import io.renku.http.tinytypes.TinyTypeURIEncoder._

  override def findProjectMembers(
      slug: projects.Slug
  )(implicit at: AccessToken): EitherT[F, ProcessingRecoverableError, Set[GitLabMember]] = EitherT {
    fetch(uri"projects" / slug / "members" / "all")(at.some)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private val endpointName: String Refined NonEmpty = "project-members"

  private def fetch(
      uri:        Uri,
      maybePage:  Option[Int] = None,
      allMembers: Set[GitLabMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabMember]] = for {
    uri                     <- uriWithPage(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- GitLabClient[F].get(uri, endpointName)(mapResponse)
    allMembers              <- addNextPage(uri, allMembers, fetchedUsersAndNextPage)
  } yield allMembers

  private def uriWithPage(uri: Uri, maybePage: Option[Int]) = maybePage match {
    case Some(page) => uri withQueryParam ("page", page)
    case None       => uri
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Set[GitLabMember], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage: Option[Int] = response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)
      response.as[List[GitLabMember]].map(_.toSet -> maybeNextPage)
    case (NotFound, _, _) => (Set.empty[GitLabMember] -> Option.empty[Int]).pure[F]
  }

  private def addNextPage(
      url:                          Uri,
      allMembers:                   Set[GitLabMember],
      fetchedUsersAndMaybeNextPage: (Set[GitLabMember], Option[Int])
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[GitLabMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetch(url, maybeNextPage, allMembers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allMembers ++ fetchedUsers).pure[F]
    }
}
