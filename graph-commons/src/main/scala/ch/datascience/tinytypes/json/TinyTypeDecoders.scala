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

package ch.datascience.tinytypes.json

import java.time.ZoneOffset.UTC
import java.time.{Instant, LocalDate, OffsetDateTime}

import cats.implicits._
import ch.datascience.tinytypes._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder.decodeString
import io.circe.{Decoder, DecodingFailure}

object TinyTypeDecoders {

  type NonBlank = String Refined NonEmpty

  def blankToNone(maybeValue: Option[String]): Option[NonBlank] = maybeValue.map(_.trim).flatMap {
    case ""       => None
    case nonBlank => RefType.applyRef[NonBlank](nonBlank).fold(e => { print(e); None }, Option.apply)
  }

  def toOption[TT <: StringTinyType](
      implicit tinyTypeFactory: From[TT]
  ): Option[NonBlank] => Either[DecodingFailure, Option[TT]] = {
    case Some(nonBlank) =>
      tinyTypeFactory
        .from(nonBlank.value)
        .map(Option.apply)
        .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
    case None => Right(None)
  }

  implicit def stringDecoder[TT <: StringTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      tinyTypeFactory.from(value).leftMap(_.getMessage)
    }

  implicit def localDateDecoder[TT <: LocalDateTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      Either
        .catchNonFatal(LocalDate.parse(value))
        .flatMap(tinyTypeFactory.from)
        .leftMap(_.getMessage)
    }

  implicit def instantDecoder[TT <: InstantTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      Either
        .catchNonFatal(OffsetDateTime.parse(value))
        .flatMap(offsetDateTime => Either.catchNonFatal(offsetDateTime.atZoneSameInstant(UTC).toInstant))
        .orElse(Either.catchNonFatal(Instant.parse(value)))
        .flatMap(tinyTypeFactory.from)
        .leftMap(_.getMessage)
    }
}
