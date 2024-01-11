package io.renku.http

import cats.effect.Concurrent
import io.circe.Json
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
}

object RenkuEntityCodec extends RenkuEntityCodec
