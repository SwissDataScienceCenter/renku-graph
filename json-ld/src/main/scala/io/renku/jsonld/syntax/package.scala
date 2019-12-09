package io.renku.jsonld

package object syntax {

  final implicit class JsonEncoderOps[Type](value: Type) {

    def asJsonLD(implicit encoder: JsonLDEncoder[Type]): JsonLD = encoder(value)
  }
}
