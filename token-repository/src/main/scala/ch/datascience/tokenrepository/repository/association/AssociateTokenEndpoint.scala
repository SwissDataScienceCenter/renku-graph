/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository.association

import cats.MonadError
import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.events.ProjectId
import ch.datascience.logging.ApplicationLogger
import ch.datascience.tokenrepository.repository.ProjectIdPathBinder
import io.chrisdavenport.log4cats.Logger
import io.circe._
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, HttpRoutes, Response}

import scala.language.higherKinds
import scala.util.control.NonFatal

class AssociateTokenEndpoint[Interpretation[_]: Effect](
    tokenAssociator: TokenAssociator[Interpretation],
    logger:          Logger[Interpretation]
)(implicit ME:       MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import tokenAssociator._

  val associateToken: HttpRoutes[Interpretation] = HttpRoutes.of[Interpretation] {
    case request @ PUT -> Root / "projects" / ProjectIdPathBinder(projectId) / "tokens" => {
      for {
        accessToken <- request.as[AccessToken].recoverWith(badRequest)
        _           <- associate(projectId, accessToken)
        result      <- toHttpResult(projectId)()
      } yield result
    } recoverWith httpResult(projectId)
  }

  private implicit lazy val accessTokenDecoder: Decoder[AccessToken] = (cursor: HCursor) => {
    for {
      maybeOauth    <- cursor.downField("oauthAccessToken").as[Option[String]].flatMap(to(OAuthAccessToken.from))
      maybePersonal <- cursor.downField("personalAccessToken").as[Option[String]].flatMap(to(PersonalAccessToken.from))
      token <- Either.fromOption(maybeOauth orElse maybePersonal,
                                 ifNone = DecodingFailure("Access token cannot be deserialized", Nil))
    } yield token
  }

  private def to[T <: AccessToken](
      from: String => Either[IllegalArgumentException, T]
  ): Option[String] => DecodingFailure Either Option[AccessToken] = {
    case None => Right(None)
    case Some(token) =>
      from(token)
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(Option.apply)
  }

  private implicit lazy val accessTokenEntityDecoder: EntityDecoder[Interpretation, AccessToken] =
    jsonOf[Interpretation, AccessToken]

  private def toHttpResult(projectId: ProjectId): Unit => Interpretation[Response[Interpretation]] = _ => {
    logger.info(s"Token associated with projectId: $projectId")
    NoContent()
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[AccessToken]] = {
    case NonFatal(exception) =>
      ME.raiseError(BadRequestError(exception))
  }

  private def httpResult(projectId: ProjectId): PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) =>
      BadRequest(ErrorMessage(exception.getMessage))
    case NonFatal(exception) =>
      val errorMessage = ErrorMessage(s"Associating token with projectId: $projectId failed")
      logger.error(exception)(errorMessage.value)
      InternalServerError(errorMessage)
  }
}

class IOAssociateTokenEndpoint(implicit contextShift: ContextShift[IO])
    extends AssociateTokenEndpoint[IO](
      new IOTokenAssociator,
      ApplicationLogger
    )
