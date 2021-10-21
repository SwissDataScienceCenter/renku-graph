/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.data.{EitherT, OptionT}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.{GitLabApiUrl, projects, users}
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.client.{AccessToken, RestClient}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import org.http4s.Method.GET
import org.http4s.Status.{Forbidden, ServiceUnavailable, Unauthorized}
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.dsl.io.{NotFound, Ok}
import org.http4s.util.CaseInsensitiveString
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

private trait ProjectInfoFinder[F[_]] {
  def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:     Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[GitLabProjectInfo]]
}

private object ProjectInfoFinder {
  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO])(implicit
      executionContext:      ExecutionContext,
      contextShift:          ContextShift[IO],
      timer:                 Timer[IO]
  ): IO[ProjectInfoFinder[IO]] = for {
    gitLabUrl <- GitLabUrlLoader[IO]()
  } yield new ProjectInfoFinderImpl(gitLabUrl.apiV4, gitLabThrottler, logger)
}

private class ProjectInfoFinderImpl(
    gitLabApiUrl:            GitLabApiUrl,
    gitLabThrottler:         Throttler[IO, GitLab],
    logger:                  Logger[IO],
    retryInterval:           FiniteDuration = RestClient.SleepAfterConnectionIssue,
    maxRetries:              Int Refined NonNegative = RestClient.MaxRetriesAfterConnectionTimeout,
    requestTimeoutOverride:  Option[Duration] = None
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RestClient(gitLabThrottler,
                       logger,
                       retryInterval = retryInterval,
                       maxRetries = maxRetries,
                       requestTimeoutOverride = requestTimeoutOverride
    )
    with ProjectInfoFinder[IO] {

  import io.circe.Decoder.decodeOption
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private type ProjectAndCreator = (GitLabProjectInfo, Option[users.GitLabId])

  override def findProjectInfo(path: projects.Path)(implicit
      maybeAccessToken:              Option[AccessToken]
  ): EitherT[IO, ProcessingRecoverableError, Option[GitLabProjectInfo]] = EitherT {
    {
      for {
        projectAndCreator <- fetchProject(path)
        maybeCreator      <- fetchCreator(projectAndCreator._2)
        members           <- OptionT.liftF(fetchMembers(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/members"))
        users             <- OptionT.liftF(fetchMembers(s"$gitLabApiUrl/projects/${urlEncode(path.value)}/users"))
      } yield projectAndCreator._1.copy(maybeCreator = maybeCreator, members = members ++ users)
    }.value.map(_.asRight[ProcessingRecoverableError]).recoverWith(maybeRecoverableError)
  }

  private def fetchProject(path: projects.Path)(implicit maybeAccessToken: Option[AccessToken]) = OptionT {
    for {
      projectsUri       <- validateUri(s"$gitLabApiUrl/projects/${urlEncode(path.value)}")
      projectAndCreator <- send(secureRequest(GET, projectsUri))(mapTo[ProjectAndCreator])
    } yield projectAndCreator
  }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[IO, OUT]
  ): PartialFunction[(Status, Request[IO], Response[IO]), IO[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply)
    case (NotFound, _, _)  => None.pure[IO]
  }

  private implicit lazy val projectDecoder: EntityDecoder[IO, ProjectAndCreator] = {

    lazy val parentPathDecoder: Decoder[projects.Path] = _.downField("path_with_namespace").as[projects.Path]

    implicit val decoder: Decoder[ProjectAndCreator] = cursor =>
      for {
        path           <- cursor.downField("path_with_namespace").as[projects.Path]
        name           <- cursor.downField("name").as[projects.Name]
        visibility     <- cursor.downField("visibility").as[projects.Visibility]
        dateCreated    <- cursor.downField("created_at").as[projects.DateCreated]
        maybeCreatorId <- cursor.downField("creator_id").as[Option[users.GitLabId]]
        maybeParentPath <- cursor
                             .downField("forked_from_project")
                             .as[Option[projects.Path]](decodeOption(parentPathDecoder))
      } yield GitLabProjectInfo(name,
                                path,
                                dateCreated,
                                maybeCreator = None,
                                members = Set.empty,
                                visibility,
                                maybeParentPath
      ) -> maybeCreatorId

    jsonOf[IO, ProjectAndCreator]
  }

  private def fetchCreator(
      maybeCreatorId:          Option[users.GitLabId]
  )(implicit maybeAccessToken: Option[AccessToken]): OptionT[IO, Option[ProjectMember]] =
    maybeCreatorId match {
      case None => OptionT.some[IO](Option.empty[ProjectMember])
      case Some(creatorId) =>
        OptionT.liftF {
          for {
            usersUri     <- validateUri(s"$gitLabApiUrl/users/$creatorId")
            maybeCreator <- send(secureRequest(GET, usersUri))(mapTo[ProjectMember])
          } yield maybeCreator
        }
    }

  private implicit val memberDecoder: Decoder[ProjectMember] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[users.GitLabId]
      name     <- cursor.downField("name").as[users.Name]
      username <- cursor.downField("username").as[users.Username]
    } yield ProjectMember(name, username, gitLabId)

  private implicit lazy val memberEntityDecoder: EntityDecoder[IO, ProjectMember] = jsonOf[IO, ProjectMember]
  private implicit lazy val membersDecoder: EntityDecoder[IO, List[ProjectMember]] = jsonOf[IO, List[ProjectMember]]

  private def fetchMembers(
      url:        String,
      maybePage:  Option[Int] = None,
      allMembers: Set[ProjectMember] = Set.empty
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): IO[Set[ProjectMember]] = for {
    uri                     <- validateUri(merge(url, maybePage))
    fetchedUsersAndNextPage <- send(secureRequest(GET, uri))(mapMembersResponse)
    allMembers              <- addNextPage(url, allMembers, fetchedUsersAndNextPage)
  } yield allMembers

  private def merge(url: String, maybePage: Option[Int] = None) =
    maybePage map (page => s"$url?page=$page") getOrElse url

  private lazy val mapMembersResponse
      : PartialFunction[(Status, Request[IO], Response[IO]), IO[(Set[ProjectMember], Option[Int])]] = {
    case (Ok, _, response) =>
      lazy val maybeNextPage: Option[Int] =
        response.headers.get(CaseInsensitiveString("X-Next-Page")).flatMap(_.value.toIntOption)

      response.as[List[ProjectMember]].map(_.toSet -> maybeNextPage)
    case (NotFound, _, _) => (Set.empty[ProjectMember] -> Option.empty[Int]).pure[IO]
  }

  private def addNextPage(
      url:                          String,
      allMembers:                   Set[ProjectMember],
      fetchedUsersAndMaybeNextPage: (Set[ProjectMember], Option[Int])
  )(implicit maybeAccessToken:      Option[AccessToken]): IO[Set[ProjectMember]] =
    fetchedUsersAndMaybeNextPage match {
      case (fetchedUsers, maybeNextPage @ Some(_)) => fetchMembers(url, maybeNextPage, allMembers ++ fetchedUsers)
      case (fetchedUsers, None)                    => (allMembers ++ fetchedUsers).pure[IO]
    }

  private lazy val maybeRecoverableError
      : PartialFunction[Throwable, IO[Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]] = {
    case exception @ (_: ConnectivityException | _: ClientException) =>
      Either.left(TransformationRecoverableError(exception.getMessage, exception.getCause)).pure[IO]
    case exception @ UnexpectedResponseException(ServiceUnavailable | Forbidden | Unauthorized, _) =>
      Either.left(TransformationRecoverableError(exception.getMessage, exception.getCause)).pure[IO]
  }
}
