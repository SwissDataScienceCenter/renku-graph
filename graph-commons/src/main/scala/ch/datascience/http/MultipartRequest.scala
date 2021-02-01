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

package ch.datascience.http

import cats.MonadError
import cats.data.EitherT
import cats.effect.Sync
import cats.implicits.{catsSyntaxApplicativeId, toTraverseOps}
import ch.datascience.http.MultipartRequest._
import fs2.text.utf8Decode
import io.circe.Json
import io.circe.parser.parse
import org.http4s.multipart.Multipart
import org.http4s.{Request, Response}

trait MultipartRequest {

  def toMultipart[Interpretation[_]: Sync, RequestContent](content: RequestContent)(implicit
      encoder: MultipartContentEncoder[RequestContent]
  ): Multipart[Interpretation] = encoder.encode[Interpretation](content)

  def decodeAndProcessMultipart[Interpretation[_]: Sync, RequestContent](
      request: Request[Interpretation],
      processResult: EitherT[Interpretation, DecodingFailure, RequestContent] => Interpretation[
        Response[Interpretation]
      ]
  )(implicit
      decoder: MultipartContentDecoder[RequestContent],
      ME:      MonadError[Interpretation, Throwable]
  ): Interpretation[Response[Interpretation]] =
    request.decode[Multipart[Interpretation]](multipart => processResult(decoder.decode[Interpretation](multipart)))

}

object MultipartRequest {
  trait MultipartContentEncoder[RequestContent] {
    def encode[Interpretation[_]: Sync](content: RequestContent): Multipart[Interpretation]
  }

  trait MultipartContentDecoder[RequestContent] {
    def decode[Interpretation[_]: Sync](
        request: Multipart[Interpretation]
    ): EitherT[Interpretation, DecodingFailure, RequestContent]
  }

  abstract class MultipartFailure(val message: String) extends Exception(message)
  final case class DecodingFailure(override val message: String) extends MultipartFailure(message)
  final case class EncodingFailure(override val message: String) extends MultipartFailure(message)

}

object EventRequest {
  case class EventRequestContent(event: Json, maybePayload: Option[String])

  implicit object EventRequestDecoder extends MultipartContentDecoder[EventRequestContent] {
    override def decode[Interpretation[_]: Sync](
        request: Multipart[Interpretation]
    ): EitherT[Interpretation, DecodingFailure, EventRequestContent] =
      (request.parts.find(_.name.contains("event")), request.parts.find(_.name.contains("payload"))) match {
        case (Some(eventPart), maybePayloadPart) =>
          for {
            eventStr <- EitherT.liftF[Interpretation, DecodingFailure, String](
                          eventPart.body.through(utf8Decode).compile.foldMonoid
                        )
            event <- EitherT
                       .fromEither[Interpretation](parse(eventStr))
                       .leftSemiflatMap(e => DecodingFailure(e.message).pure[Interpretation])
            maybePayload <- EitherT.liftF[Interpretation, DecodingFailure, Option[String]](
                              maybePayloadPart.map(_.body.through(utf8Decode).compile.foldMonoid).sequence
                            )
          } yield EventRequestContent(event, maybePayload)
        case _ => EitherT.leftT(DecodingFailure("Multipart request malformed"))
      }
  }
}
