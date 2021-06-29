package io.renku.jsonld

import io.circe.Json

package object parser extends Parser {
  private val parser = new JsonLDParser()

  override def parse(string: String): Either[ParsingFailure, JsonLD] = parser.parse(string)
  override def parse(json:   Json):   Either[ParsingFailure, JsonLD] = parser.parse(json)
}
