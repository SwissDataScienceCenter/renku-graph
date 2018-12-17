/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.controllers

import javax.inject.{ Inject, Singleton }
import play.api.Logger
import play.api.http.HttpErrorHandler
import play.api.http.Status._
import play.api.libs.json.{ JsValue, Json }
import play.api.mvc.{ RequestHeader, Result, Results }

import scala.concurrent.{ ExecutionContext, Future }

@Singleton
class JsonHttpErrorHandler @Inject() ()( implicit executionContext: ExecutionContext ) extends HttpErrorHandler {

  override def onClientError( request: RequestHeader, statusCode: Int, message: String ): Future[Result] = Future {
    Logger.error( s"${request.method} ${request.uri} for request ${request.id} failed with message: $message" )

    result(
      status = statusCode,
      body   = Json.obj(
        "requestId" -> request.id,
        "statusCode" -> statusCode,
        "message" -> message
      )
    )
  }

  override def onServerError( request: RequestHeader, exception: Throwable ): Future[Result] = Future {
    Logger.error( s"${request.method} ${request.uri} for request ${request.id} failed", exception )

    result(
      status = INTERNAL_SERVER_ERROR,
      body   = Json.obj(
        "requestId" -> request.id,
        "statusCode" -> INTERNAL_SERVER_ERROR,
        "message" -> exception.getMessage
      )
    )
  }

  private def result( status: Int, body: JsValue ): Result = new Results.Status( status )( body )
}
