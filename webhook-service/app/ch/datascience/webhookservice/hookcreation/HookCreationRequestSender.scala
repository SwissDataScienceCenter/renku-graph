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

package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import ch.datascience.graph.events.ProjectId
import ch.datascience.webhookservice.hookcreation.HookCreationRequestSender.UnauthorizedException
import ch.datascience.webhookservice.model.UserAuthToken
import ch.datascience.webhookservice.routes.PushEventConsumer
import javax.inject.{ Inject, Singleton }

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private abstract class HookCreationRequestSender[Interpretation[_]] {
  def createHook( projectId: ProjectId, authToken: UserAuthToken ): Interpretation[Unit]
}

private object HookCreationRequestSender {
  final case object UnauthorizedException extends RuntimeException( "Unauthorized" )
}

@Singleton
private class IOHookCreationRequestSender @Inject() ( configProvider: IOHookCreationConfigProvider )( implicit executionContext: ExecutionContext )
  extends HookCreationRequestSender[IO] {

  import cats.effect._
  import io.circe.Json
  import org.http4s.Method.POST
  import org.http4s.Status.{ Created, Unauthorized }
  import org.http4s.circe._
  import org.http4s.client.blaze.BlazeClientBuilder
  import org.http4s.{ Header, Headers, Request, Uri }

  private implicit val cs: ContextShift[IO] = IO.contextShift( executionContext )
  private val F = implicitly[ConcurrentEffect[IO]]

  def createHook( projectId: ProjectId, authToken: UserAuthToken ): IO[Unit] = for {
    config <- configProvider.get()
    payload = Json.obj(
      "id" -> Json.fromInt( projectId.value ),
      "url" -> Json.fromString( s"${config.selfUrl}${PushEventConsumer.processPushEvent().url}" ),
      "push_events" -> Json.fromBoolean( true )
    )
    uri <- F.fromEither( Uri.fromString( s"${config.gitLabUrl}/api/v4/projects/$projectId/hooks" ) )
    request = Request[IO](
      method  = POST,
      uri     = uri,
      headers = Headers( Header( "PRIVATE-TOKEN", authToken.value ) )
    ).withEntity( payload )

    result <- BlazeClientBuilder[IO]( executionContext ).resource.use { httpClient =>
      httpClient.fetch[Unit]( request ) { response =>
        response.status match {
          case Created      => F.pure( () )
          case Unauthorized => F.raiseError( UnauthorizedException )
          case other =>
            F.flatMap( response.as[String] ) { bodyAsString =>
              F.raiseError( new RuntimeException( s"${request.method} ${request.uri} returned $other; body: ${bodyAsString.split( '\n' ).map( _.trim.filter( _ >= ' ' ) ).mkString}" ) )
            }
        }
      }
    }
  } yield result
}
