package io.renku.jsonld.parser

import io.circe.Json
import io.renku.jsonld.JsonLD

trait Parser {
  def parse(string: String): Either[ParsingFailure, JsonLD]
  def parse(json:   Json):   Either[ParsingFailure, JsonLD]
}
