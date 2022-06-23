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

package io.renku.triplesgenerator.events.categories.tsprovisioning.projectinfo

import cats.NonEmptyParallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsprovisioning.RecoverableErrorsRecovery
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.typelevel.ci._
import org.typelevel.log4cats.Logger

private trait ProjectMembersFinder[F[_]] {
  def findProjectMembers(path: projects.Path)(implicit
      maybeAccessToken:        Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Set[ProjectMember]]
}

private object ProjectMembersFinder {
  def apply[F[_]: Async: NonEmptyParallel: GitLabClient: Logger]: F[ProjectMembersFinder[F]] =
    new ProjectMembersFinderImpl[F].pure[F].widen[ProjectMembersFinder[F]]
}

private class ProjectMembersFinderImpl[F[_]: Async: NonEmptyParallel: GitLabClient: Logger](
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectMembersFinder[F] {

  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def findProjectMembers(path: projects.Path)(implicit
      maybeAccessToken:                 Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Set[ProjectMember]] = EitherT {
    (
      fetch(uri"projects" / path.show / "members", "project-members"),
      fetch(uri"projects" / path.show / "users", "project-users")
    ).parMapN(_ ++ _)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private implicit val memberDecoder: Decoder[ProjectMember] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[persons.GitLabId]
      name     <- cursor.downField("name").as[persons.Name]
      username <- cursor.downField("username").as[persons.Username]
    } yield ProjectMember(name, username, gitLabId)

  private implicit lazy val memberEntityDecoder: EntityDecoder[F, ProjectMember]       = jsonOf[F, ProjectMember]
  private implicit lazy val membersDecoder:      EntityDecoder[F, List[ProjectMember]] = jsonOf[F, List[ProjectMember]]

  private def fetch(
      uri:                     Uri,
      endpointName:            String Refined NonEmpty,
      maybePage:               Option[Int] = None,
      allMembers:              Set[ProjectMember] = Set.empty
  )(implicit maybeAccessToken: Option[AccessToken]): F[Set[ProjectMember]] = for {
    uri                     <- uriWithPage(uri, maybePage).pure[F]
    fetchedUsersAndNextPage <- GitLabClient[F].get(uri, endpointName)(mapResponse)
    allMembers              <- addNextPage(uri, endpointName, allMembers, fetchedUsersAndNextPage)
  } yield allMembers

  private def uriWithPage(uri: Uri, maybePage: Option[Int]) = maybePage match {
    case Some(page) => uri withQueryParam ("page", page)
    case None       => uri
  }

  private lazy val mapResponse
      : PartialFunction[(Status, Request[F], Response[F]), F[(Set[ProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage: Option[Int] = response.headers.get(ci"X-Next-Page").flatMap(_.head.value.toIntOption)
      response.as[List[ProjectMember]].map(_.toSet -> maybeNextPage)
    case (NotFound, _, _) => (Set.empty[ProjectMember] -> Option.empty[Int]).pure[F]
  }

  private def addNextPage(
      url:                          Uri,
      endpointName:                 String Refined NonEmpty,
      allMembers:                   Set[ProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[ProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): F[Set[ProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) =>
        fetch(url, endpointName, maybeNextPage, allMembers ++ fetchedUsers)
      case (fetchedUsers, None) => (allMembers ++ fetchedUsers).pure[F]
    }
}
