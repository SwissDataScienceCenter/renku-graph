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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage._
import ch.datascience.graph.model.events.ProjectId
import ch.datascience.webhookservice.exceptions.UnauthorizedException
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult
import ch.datascience.webhookservice.hookcreation.HookCreator.HookCreationResult.{HookCreated, HookExisted}
import ch.datascience.webhookservice.security.AccessTokenExtractor
import org.http4s.dsl.Http4sDsl
import org.http4s.{Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.control.NonFatal

class HookCreationEndpoint[Interpretation[_]: Effect](
    hookCreator:       HookCreator[Interpretation],
    accessTokenFinder: AccessTokenExtractor[Interpretation]
) extends Http4sDsl[Interpretation] {

  import accessTokenFinder._

  def createHook(projectId: ProjectId, request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      accessToken    <- findAccessToken(request)
      creationResult <- hookCreator.createHook(projectId, accessToken)
      response       <- toHttpResponse(creationResult)
    } yield response
  } recoverWith httpResponse

  private lazy val toHttpResponse: HookCreationResult => Interpretation[Response[Interpretation]] = {
    case HookCreated => Created()
    case HookExisted => Ok()
  }

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case ex @ UnauthorizedException =>
      Response[Interpretation](Status.Unauthorized)
        .withEntity[ErrorMessage](ErrorMessage(ex.getMessage))
        .pure[Interpretation]
    case NonFatal(exception) => InternalServerError(ErrorMessage(exception.getMessage))
  }
}

class IOHookCreationEndpoint(
    implicit executionContext: ExecutionContext,
    contextShift:              ContextShift[IO]
) extends HookCreationEndpoint[IO](new IOHookCreator, new AccessTokenExtractor[IO])
