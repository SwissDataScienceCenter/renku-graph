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

package io.renku.rdfstore

import cats.syntax.all._
import io.circe.Decoder.{Result, decodeList}
import io.circe.{Decoder, HCursor}

object ResultsDecoding extends ResultsDecoding

trait ResultsDecoding {

  object ListResultsDecoder {
    def apply[OUT](rowDecoder: Decoder[OUT]): Decoder[List[OUT]] =
      _.downField("results").downField("bindings").as(decodeList(rowDecoder))
  }

  object OptionalResultDecoder {
    def apply[OUT](onMultiple: String)(rowDecoder: Decoder[OUT]): Decoder[Option[OUT]] =
      ListResultsDecoder[OUT](rowDecoder).emap {
        case Nil           => Option.empty[OUT].asRight
        case single :: Nil => Option(single).asRight
        case _             => onMultiple.asLeft
      }
  }

  object UniqueResultDecoder {
    def apply[OUT](onEmpty: String, onMultiple: String)(rowDecoder: Decoder[OUT]): Decoder[OUT] =
      ListResultsDecoder[OUT](rowDecoder).emap {
        case Nil           => onEmpty.asLeft
        case single :: Nil => single.asRight
        case _             => onMultiple.asLeft
      }
  }

  def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}
