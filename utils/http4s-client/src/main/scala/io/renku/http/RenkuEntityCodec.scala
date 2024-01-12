/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.http

import cats.effect.Concurrent
import io.circe.{Decoder, Encoder, Json}
import io.renku.data.Message
import org.http4s.{EntityDecoder, EntityEncoder, circe}

trait RenkuEntityCodec {

  implicit def stringEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, String] =
    EntityDecoder.text[F]

  implicit def stringEntityEncoder[F[_]]: EntityEncoder[F, String] =
    EntityEncoder.stringEncoder[F]

  implicit def jsonDecoder[F[_]: Concurrent]: circe.JsonDecoder[F] =
    circe.JsonDecoder.impl[F]

  implicit def jsonEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, Json] =
    circe.jsonDecoder[F]

  implicit def jsonEntityEncoder[F[_]]: EntityEncoder[F, Json] =
    circe.jsonEncoder[F]

  implicit def messageEntityDecoder[F[_]: Concurrent]: EntityDecoder[F, Message] =
    circe.CirceEntityDecoder.circeEntityDecoder[F, Message]

  implicit def messageEntityEncoder[F[_]]: EntityEncoder[F, Message] =
    circe.CirceEntityEncoder.circeEntityEncoder[F, Message]

  def jsonEntityDecoderFor[F[_]: Concurrent, A: Decoder]: EntityDecoder[F, A] =
    circe.jsonOf[F, A]

  def jsonEntityEncoderFor[F[_], A: Encoder]: EntityEncoder[F, A] =
    circe.jsonEncoderOf[F, A]

  final implicit class MessageDecoderOps[F[_]: Concurrent](self: org.http4s.Message[F]) {
    def asJson[A](implicit decoder: Decoder[A]): F[A] =
      self.as[A](Concurrent[F], jsonEntityDecoderFor[F, A])

    def toJson[A](implicit decoder: Decoder[A]): F[A] = asJson[A]
  }
}

object RenkuEntityCodec extends RenkuEntityCodec
