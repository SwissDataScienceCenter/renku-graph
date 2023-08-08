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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.projectinfo

import cats.data.{EitherT, OptionT}
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsprovisioning.RecoverableErrorsRecovery
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.implicits._
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.log4cats.Logger

import java.time.Instant

private trait ProjectFinder[F[_]] {
  def findProject(slug: projects.Slug)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private object ProjectFinder {
  def apply[F[_]: Async: GitLabClient: Logger]: F[ProjectFinder[F]] =
    new ProjectFinderImpl[F].pure[F].widen[ProjectFinder[F]]
}

private class ProjectFinderImpl[F[_]: Async: GitLabClient: Logger](
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectFinder[F] {

  import io.circe.Decoder.decodeOption
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s.circe.jsonOf

  private type ProjectAndCreator = (GitLabProjectInfo, Option[persons.GitLabId])

  override def findProject(
      slug: projects.Slug
  )(implicit mat: Option[AccessToken]): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]] = EitherT {
    {
      for {
        (project, maybeCreatorId) <- fetchProject(slug)
        maybeCreator              <- fetchCreator(maybeCreatorId)
      } yield project.copy(maybeCreator = maybeCreator)
    }.value.map(_.asRight[ProcessingRecoverableError]).recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private def fetchProject(slug: projects.Slug)(implicit maybeAccessToken: Option[AccessToken]) = OptionT {
    GitLabClient[F].get(uri"projects" / slug, "single-project")(mapTo[ProjectAndCreator])
  }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[F, OUT]
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply)
    case (NotFound, _, _)  => Option.empty[OUT].pure[F]
  }

  private implicit lazy val projectDecoder: EntityDecoder[F, ProjectAndCreator] = {

    lazy val parentSlugDecoder: Decoder[projects.Slug] = _.downField("path_with_namespace").as[projects.Slug]

    implicit val decoder: Decoder[ProjectAndCreator] = cursor =>
      for {
        id               <- cursor.downField("id").as[projects.GitLabId]
        slug             <- cursor.downField("path_with_namespace").as[projects.Slug]
        name             <- cursor.downField("name").as[projects.Name]
        maybeVisibility  <- cursor.downField("visibility").as[Option[projects.Visibility]]
        dateCreated      <- cursor.downField("created_at").as[projects.DateCreated]
        updatedAt        <- cursor.downField("updated_at").as[Option[Instant]]
        lastActivityAt   <- cursor.downField("last_activity_at").as[Option[Instant]]
        maybeDescription <- cursor.downField("description").as[Option[projects.Description]]
        keywords         <- cursor.downField("topics").as[Set[Option[projects.Keyword]]].map(_.flatten)
        maybeCreatorId   <- cursor.downField("creator_id").as[Option[persons.GitLabId]]
        maybeParentSlug <- cursor
                             .downField("forked_from_project")
                             .as[Option[projects.Slug]](decodeOption(parentSlugDecoder))
        avatarUrl <- cursor.downField("avatar_url").as[Option[ImageUri]]
      } yield GitLabProjectInfo(
        id,
        name,
        slug,
        dateCreated,
        projects.DateModified(List(updatedAt, lastActivityAt, dateCreated.value.some).max.head),
        maybeDescription,
        maybeCreator = None,
        keywords,
        members = Set.empty,
        maybeVisibility getOrElse projects.Visibility.Public,
        maybeParentSlug,
        avatarUrl
      ) -> maybeCreatorId

    jsonOf[F, ProjectAndCreator]
  }

  private def fetchCreator(
      maybeCreatorId: Option[persons.GitLabId]
  )(implicit maybeAccessToken: Option[AccessToken]): OptionT[F, Option[ProjectMember]] =
    maybeCreatorId match {
      case None => OptionT.some[F](Option.empty[ProjectMember])
      case Some(creatorId) =>
        OptionT.liftF {
          GitLabClient[F].get(uri"users" / creatorId, "single-user")(mapTo[ProjectMember])
        }
    }

  private implicit val memberDecoder: Decoder[ProjectMember] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[persons.GitLabId]
      name     <- cursor.downField("name").as[persons.Name]
      username <- cursor.downField("username").as[persons.Username]
    } yield ProjectMember(name, username, gitLabId)

  private implicit lazy val memberEntityDecoder: EntityDecoder[F, ProjectMember] = jsonOf[F, ProjectMember]
}
