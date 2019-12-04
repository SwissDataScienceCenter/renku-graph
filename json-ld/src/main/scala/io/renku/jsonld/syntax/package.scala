package io.renku.jsonld

import io.circe.{Encoder, Json}

package object syntax {

  final implicit class JsonEncoderOps[Type](value: Type) {

    def asJsonLDValue(implicit encoder: Encoder[Type]): Json = encoder(value)
  }
}
