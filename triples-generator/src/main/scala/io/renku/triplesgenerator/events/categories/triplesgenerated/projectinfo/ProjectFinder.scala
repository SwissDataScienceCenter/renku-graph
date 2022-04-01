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

import cats.data.{EitherT, OptionT}
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.http4s.Method.GET
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EntityDecoder, Request, Response, Status}
import org.typelevel.log4cats.Logger

private trait ProjectFinder[F[_]] {
  def findProject(path: projects.Path)(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private object ProjectFinder {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[ProjectFinder[F]] = new ProjectFinderImpl(
    gitLabClient
  ).pure[F].widen[ProjectFinder[F]]
}

private class ProjectFinderImpl[F[_]: Async: Logger](
    gitLabClient:     GitLabClient[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ProjectFinder[F] {

  import io.circe.Decoder.decodeOption
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import org.http4s.circe.jsonOf

  private type ProjectAndCreator = (GitLabProjectInfo, Option[persons.GitLabId])

  override def findProject(path: projects.Path)(implicit
      maybeAccessToken:          Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]] = EitherT {
    {
      for {
        projectAndCreator <- fetchProject(path)
        maybeCreator      <- fetchCreator(projectAndCreator._2)
      } yield projectAndCreator._1.copy(maybeCreator = maybeCreator)
    }.value.map(_.asRight[ProcessingRecoverableError]).recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private def fetchProject(path: projects.Path)(implicit maybeAccessToken: Option[AccessToken]) = OptionT {
    gitLabClient.send(GET, uri"projects" / path.show, "project")(mapTo[ProjectAndCreator])
  }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[F, OUT]
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply)
    case (NotFound, _, _)  => Option.empty[OUT].pure[F]
  }

  private implicit lazy val projectDecoder: EntityDecoder[F, ProjectAndCreator] = {

    lazy val parentPathDecoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]

    implicit val decoder: Decoder[ProjectAndCreator] = cursor =>
      for {
        id               <- cursor.downField("id").as[projects.Id]
        path             <- cursor.downField("path_with_namespace").as[projects.Path]
        name             <- cursor.downField("name").as[projects.Name]
        maybeVisibility  <- cursor.downField("visibility").as[Option[projects.Visibility]]
        dateCreated      <- cursor.downField("created_at").as[projects.DateCreated]
        maybeDescription <- cursor.downField("description").as[Option[projects.Description]]
        keywords         <- cursor.downField("tag_list").as[Set[Option[projects.Keyword]]].map(_.flatten)
        maybeCreatorId   <- cursor.downField("creator_id").as[Option[persons.GitLabId]]
        maybeParentPath <- cursor
                             .downField("forked_from_project")
                             .as[Option[projects.Path]](decodeOption(parentPathDecoder))
      } yield GitLabProjectInfo(
        id,
        name,
        path,
        dateCreated,
        maybeDescription,
        maybeCreator = None,
        keywords,
        members = Set.empty,
        maybeVisibility getOrElse projects.Visibility.Public,
        maybeParentPath
      ) -> maybeCreatorId

    jsonOf[F, ProjectAndCreator]
  }

  private def fetchCreator(
      maybeCreatorId:          Option[persons.GitLabId]
  )(implicit maybeAccessToken: Option[AccessToken]): OptionT[F, Option[ProjectMember]] =
    maybeCreatorId match {
      case None => OptionT.some[F](Option.empty[ProjectMember])
      case Some(creatorId) =>
        OptionT.liftF {
          gitLabClient.send(GET, uri"users" / creatorId.show, "user")(mapTo[ProjectMember])
        }
    }

  private implicit val memberDecoder: Decoder[ProjectMember] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[persons.GitLabId]
      name     <- cursor.downField("name").as[persons.Name]
      username <- cursor.downField("username").as[persons.Username]
    } yield ProjectMember(name, username, gitLabId)

  private implicit lazy val memberEntityDecoder: EntityDecoder[F, ProjectMember]       = jsonOf[F, ProjectMember]
  private implicit lazy val membersDecoder:      EntityDecoder[F, List[ProjectMember]] = jsonOf[F, List[ProjectMember]]
}
