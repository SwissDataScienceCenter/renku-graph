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

import cats.Id
import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.Decoder.{Result, decodeList}
import io.circe.{Decoder, HCursor}

object ResultsDecoder extends ResultsDecoder

trait ResultsDecoder {

  object ResultsDecoder {
    def apply[F[_], OUT](rowDecoder: Decoder[OUT])(implicit toF: List[OUT] => Either[String, F[OUT]]): Decoder[F[OUT]] =
      _.downField("results").downField("bindings").as(decodeList(rowDecoder).emap(toF))

    def single[OUT](rowDecoder: Decoder[OUT])(implicit toF: List[OUT] => Either[String, Id[OUT]]): Decoder[OUT] =
      apply[Id, OUT](rowDecoder)
    def singleWithErrors[OUT](onEmpty: => String, onMultiple: => String)(rowDecoder: Decoder[OUT]): Decoder[OUT] =
      apply[Id, OUT](rowDecoder)(toSingle(onEmpty, onMultiple))
  }

  implicit def toList[OUT]: List[OUT] => Either[String, List[OUT]] = _.asRight

  def toNonEmptyList[OUT](onEmpty: => String): List[OUT] => Either[String, NonEmptyList[OUT]] = {
    case Nil          => onEmpty.asLeft
    case head :: tail => NonEmptyList.of(head, tail: _*).asRight
  }
  implicit def toNonEmptyList[OUT]: List[OUT] => Either[String, NonEmptyList[OUT]] =
    toNonEmptyList(onEmpty = "No records found but expected at least one")

  def toOption[OUT](onMultiple: List[OUT] => String): List[OUT] => Either[String, Option[OUT]] = {
    case Nil           => Option.empty[OUT].asRight
    case single :: Nil => Option(single).asRight
    case multi         => onMultiple(multi).asLeft
  }
  def toOption[OUT](onMultiple: => String): List[OUT] => Either[String, Option[OUT]] =
    toOption[OUT]((_: List[OUT]) => onMultiple)
  implicit def toOption[OUT]: List[OUT] => Either[String, Option[OUT]] =
    toOption[OUT]((_: List[OUT]) => "Multiple records found but expected one or zero")

  def toSingle[OUT](onEmpty: String, onMultiple: List[OUT] => String): List[OUT] => Either[String, Id[OUT]] = {
    case Nil           => onEmpty.asLeft
    case single :: Nil => single.asRight
    case multi         => onMultiple(multi).asLeft
  }
  def toSingle[OUT](onEmpty: => String, onMultiple: => String): List[OUT] => Either[String, Id[OUT]] =
    toSingle[OUT](onEmpty, (_: List[OUT]) => onMultiple)
  implicit def toSingle[OUT]: List[OUT] => Either[String, Id[OUT]] =
    toSingle[OUT](onEmpty = "No records found but expected exactly one",
                  (_: List[OUT]) => "Multiple records found but expected exactly one"
    )

  def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}
