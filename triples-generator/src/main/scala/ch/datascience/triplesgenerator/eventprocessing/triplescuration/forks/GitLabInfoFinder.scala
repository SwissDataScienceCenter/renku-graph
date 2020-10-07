/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.GitLabUrl
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.graph.model.users.Email
import ch.datascience.http.client.{AccessToken, IORestClient}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait GitLabInfoFinder[Interpretation[_]] {

  def findProject(path: Path)(implicit maybeAccessToken: Option[AccessToken]): Interpretation[Option[GitLabProject]]
}

private class IOGitLabInfoFinder(
    gitLabUrl:               GitLabUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORestClient(gitLabThrottler, logger)
    with GitLabInfoFinder[IO] {

  import cats.data.OptionT
  import cats.effect._
  import cats.syntax.all._
  import ch.datascience.http.client.UrlEncoder.urlEncode
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import io.circe._
  import Decoder._
  import org.http4s.Method.GET
  import org.http4s._
  import org.http4s.circe.jsonOf
  import org.http4s.dsl.io._

  private type ProjectAndCreatorId = (GitLabProject, Option[Int])

  override def findProject(
      path:                    projects.Path
  )(implicit maybeAccessToken: Option[AccessToken]): IO[Option[GitLabProject]] = {
    for {
      projectsUri <- OptionT.liftF(validateUri(s"$gitLabUrl/api/v4/projects/${urlEncode(path.value)}"))
      (project, maybeCreatorId) <-
        send(request(GET, projectsUri, maybeAccessToken))(mapTo[ProjectAndCreatorId]).toOptionT
      maybeCreator <- fetchCreator(maybeCreatorId, maybeAccessToken)
    } yield project.copy(maybeCreator = maybeCreator)
  }.value

  private def fetchCreator(maybeCreatorId:   Option[Int],
                           maybeAccessToken: Option[AccessToken]
  ): OptionT[IO, Option[GitLabCreator]] =
    maybeCreatorId match {
      case None => OptionT.some[IO](Option.empty[GitLabCreator])
      case Some(creatorId) =>
        OptionT.liftF {
          for {
            usersUri     <- validateUri(s"$gitLabUrl/api/v4/users/$creatorId")
            maybeCreator <- send(request(GET, usersUri, maybeAccessToken))(mapTo[GitLabCreator])
          } yield maybeCreator
        }
    }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[IO, OUT]
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply)
    case (NotFound, _, _)  => None.pure[IO]
  }

  private implicit lazy val projectDecoder: EntityDecoder[IO, ProjectAndCreatorId] = {

    lazy val parentPathDecoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]

    implicit val decoder: Decoder[ProjectAndCreatorId] = cursor =>
      for {
        path           <- cursor.downField("path_with_namespace").as[projects.Path]
        dateCreated    <- cursor.downField("created_at").as[DateCreated]
        maybeCreatorId <- cursor.downField("creator_id").as[Option[Int]]
        maybeParentPath <- cursor
                             .downField("forked_from_project")
                             .as[Option[projects.Path]](decodeOption(parentPathDecoder))
      } yield GitLabProject(path, maybeParentPath, maybeCreator = None, dateCreated) -> maybeCreatorId

    jsonOf[IO, ProjectAndCreatorId]
  }

  private implicit lazy val creatorDecoder: EntityDecoder[IO, GitLabCreator] = {
    import ch.datascience.graph.model.users

    implicit val decoder: Decoder[GitLabCreator] = cursor =>
      for {
        maybeName <- cursor.downField("name").as[Option[users.Name]]
        maybeEmail <- cursor
                        .downField("email")
                        .as[Option[String]]
                        .map(blankToNone)
                        .flatMap(toOption[Email])
        maybePublicEmail <- cursor
                              .downField("public_email")
                              .as[Option[String]]
                              .map(blankToNone)
                              .flatMap(toOption[Email])
      } yield GitLabCreator(maybeEmail orElse maybePublicEmail, maybeName)

    jsonOf[IO, GitLabCreator]
  }

  private implicit class IOOptionOps[T](io: IO[Option[T]]) {
    lazy val toOptionT: OptionT[IO, T] = OptionT(io)
  }
}

private object IOGitLabInfoFinder {

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[GitLabInfoFinder[IO]] =
    for {
      gitLabUrl <- GitLabUrl[IO]()
    } yield new IOGitLabInfoFinder(gitLabUrl, gitLabThrottler, logger)
}
