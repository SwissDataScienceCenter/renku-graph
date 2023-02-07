/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.views

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder
import io.renku.jsonld.JsonLDDecoder.{decodeOption, decodeString}
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{From, StringTinyType, TinyTypeFactory}

trait NonBlankTTJsonLDOps[TT <: StringTinyType] extends JsonLDTinyTypeEncoder[TT] with StringTinyTypeJsonLDDecoders {
  self: TinyTypeFactory[TT] with NonBlank[TT] =>

  implicit lazy val optionalTTDecoder: JsonLDDecoder[Option[TT]] =
    decodeBlankStringToNone[TT](self)

  val failIfNone: Option[TT] => JsonLDDecoder.Result[TT] = {
    case Some(v) => v.asRight
    case None    => DecodingFailure(s"A value of '$typeName' expected but got any", Nil).asLeft
  }
}

object StringTinyTypeJsonLDDecoders extends StringTinyTypeJsonLDDecoders

trait StringTinyTypeJsonLDDecoders {

  implicit def decodeBlankStringToNone[TT <: StringTinyType](implicit ttFactory: From[TT]): JsonLDDecoder[Option[TT]] =
    decodeOption(decodeString).emap((blankToNone andThen toOption)(_).leftMap(_.getMessage()))

  private lazy val blankToNone: Option[String] => Option[String] = _.map(_.trim) >>= {
    case ""       => None
    case nonBlank => Some(nonBlank)
  }

  private def toOption[TT <: StringTinyType](implicit
      ttFactory: From[TT]
  ): Option[String] => Either[DecodingFailure, Option[TT]] = {
    case Some(nonBlank) =>
      ttFactory.from(nonBlank).map(Option.apply).leftMap(exception => DecodingFailure(exception.getMessage, Nil))
    case None => Right(None)
  }
}
