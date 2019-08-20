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

package ch.datascience.http.server

import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Sync}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.controllers.ErrorMessage.ErrorMessage
import io.circe.{Decoder, DecodingFailure, Json}
import org.http4s.circe.{jsonEncoderOf, jsonOf}
import org.http4s.{EntityDecoder, EntityEncoder, HttpRoutes, Request, Response, Status}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

object EndpointTester {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  implicit val jsonEntityDecoder: EntityDecoder[IO, Json] = jsonOf[IO, Json]
  implicit val jsonEntityEncoder: EntityEncoder[IO, Json] = jsonEncoderOf[IO, Json]

  implicit class EndpointOps(endpoint: Kleisli[IO, Request[IO], Response[IO]]) {

    def call(request: Request[IO]) = new {
      private val runResponse: Response[IO] = endpoint.run(request).unsafeRunSync()

      lazy val status: Status = runResponse.status

      def body[T](implicit decoder: EntityDecoder[IO, T]): T = runResponse.as[T].unsafeRunSync
    }
  }

  val notAvailableResponse: Response[IO] = Response(Status.ServiceUnavailable)

  implicit class RoutesOps(routes: HttpRoutes[IO]) {
    def or(response: Response[IO]): Kleisli[IO, Request[IO], Response[IO]] =
      Kleisli(a => routes.run(a).getOrElse(response))
  }

  implicit val errorMessageDecoder: Decoder[ErrorMessage] = Decoder.instance[ErrorMessage] {
    _.downField("message")
      .as[String]
      .flatMap(message => Either.fromTry(Try(ErrorMessage(message))))
      .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
  }

  implicit def errorMessageEntityDecoder[F[_]: Sync]: EntityDecoder[F, ErrorMessage] = jsonOf[F, ErrorMessage]
}
