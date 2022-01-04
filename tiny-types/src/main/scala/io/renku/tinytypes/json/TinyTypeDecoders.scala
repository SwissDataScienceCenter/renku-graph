/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes.json

import cats.syntax.all._
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.collection.NonEmpty
import io.circe.Decoder._
import io.circe.{Decoder, DecodingFailure}
import io.renku.tinytypes._

import java.time.ZoneOffset.UTC
import java.time._

object TinyTypeDecoders {

  type NonBlank = String Refined NonEmpty

  implicit def stringDecoder[TT <: StringTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      tinyTypeFactory.from(value).leftMap(_.getMessage)
    }

  implicit def relativePathDecoder[TT <: RelativePathTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      tinyTypeFactory.from(value).leftMap(_.getMessage)
    }

  implicit def urlDecoder[TT <: UrlTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      tinyTypeFactory.from(value).leftMap(_.getMessage)
    }

  implicit def intDecoder[TT <: IntTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeInt.emap { value =>
      tinyTypeFactory.from(value).leftMap(_.getMessage)
    }

  implicit def longDecoder[TT <: LongTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeLong.emap { value =>
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

  implicit def durationDecoder[TT <: DurationTinyType](implicit tinyTypeFactory: From[TT]): Decoder[TT] =
    decodeString.emap { value =>
      Either
        .catchNonFatal(Duration.parse(value))
        .flatMap(tinyTypeFactory.from)
        .leftMap(_.getMessage)
    }

  implicit def blankStringToNoneDecoder[TT <: StringTinyType](implicit tinyTypeFactory: From[TT]): Decoder[Option[TT]] =
    decodeOption(decodeString).emap((blankToNone andThen toOption[TT])(_).leftMap(_.getMessage()))

  private lazy val blankToNone: Option[String] => Option[NonBlank] = _.map(_.trim) >>= {
    case ""       => None
    case nonBlank => RefType.applyRef[NonBlank](nonBlank).fold(_ => None, Option.apply)
  }

  private def toOption[TT <: TinyType { type V = String }](implicit
      tinyTypeFactory: From[TT]
  ): Option[NonBlank] => Either[DecodingFailure, Option[TT]] = {
    case Some(nonBlank) =>
      tinyTypeFactory
        .from(nonBlank.value)
        .map(Option.apply)
        .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
    case None => Right(None)
  }
}
