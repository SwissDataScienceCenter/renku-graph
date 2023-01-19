package io.renku.cli.model

import io.renku.jsonld.JsonLDDecoder

trait FlatJsonLDDecoder[A] extends JsonLDDecoder[List[A]] {

}

object FlatJsonLDDecoder {

  def apply[A](decoder: JsonLDDecoder[A]): FlatJsonLDDecoder[A] = cursor => {
    println(s"$decoder $cursor")

    ???
  }
}
