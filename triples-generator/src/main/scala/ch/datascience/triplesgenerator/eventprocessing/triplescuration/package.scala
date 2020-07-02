/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.{EitherT, OptionT}
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.tinytypes.TinyType
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import io.circe.Json

import scala.language.higherKinds

package object triplescuration {

  private[eventprocessing] type CurationResults[Interpretation[_]] =
    EitherT[Interpretation, ProcessingRecoverableError, CuratedTriples]

  def `INSERT DATA`[TT <: TinyType { type V = String }](resource: String, property: String, value: TT): String =
    s"INSERT DATA { $resource $property '${sparqlEncode(value.value)}'}"

  implicit class JsonOps(json: Json) {
    import cats.implicits._
    import io.circe.{Decoder, Encoder}
    import io.circe.Decoder.decodeList
    import io.circe.Encoder.encodeList
    import io.circe.optics.JsonPath.root

    def get[T](property: String)(implicit decode: Decoder[T], encode: Encoder[T]): Option[T] =
      root.selectDynamic(property).as[T].getOption(json)

    def getValues[T](
        property:      String
    )(implicit decode: Decoder[T], encode: Encoder[T]): List[T] = {
      import io.circe.literal._

      val valuesDecoder: Decoder[T] = _.downField("@value").as[T]
      val valuesEncoder: Encoder[T] = Encoder.instance[T](value => json"""{"@value": $value}""")
      val findListOfValues = root
        .selectDynamic(property)
        .as[List[T]](decodeList(valuesDecoder), encodeList(valuesEncoder))
        .getOption(json)
      val findSingleValue = root
        .selectDynamic(property)
        .as[T](valuesDecoder, valuesEncoder)
        .getOption(json)

      findListOfValues orElse findSingleValue.map(List(_)) getOrElse List.empty
    }

    def getValue[F[_], T](
        property:      String
    )(implicit decode: Decoder[T], encode: Encoder[T], ME: MonadError[F, Throwable]): OptionT[F, T] =
      getValues(property)(decode, encode) match {
        case Nil      => OptionT.none[F, T]
        case x +: Nil => OptionT.some[F](x)
        case _        => OptionT.liftF(new IllegalStateException(s"Multiple values found for $property").raiseError[F, T])
      }

    def remove(property: String): Json = root.obj.modify(_.remove(property))(json)
  }
}
