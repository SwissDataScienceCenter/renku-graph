/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration

import cats.MonadError
import cats.data.{EitherT, OptionT}
import cats.syntax.all._
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.tinytypes.TinyType
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.CommitEventProcessor.ProcessingRecoverableError
import io.circe.Decoder.decodeString
import io.circe.{Decoder, Json}
import io.renku.jsonld.{EntityId, Property}

package object triplescuration {

  private[awaitinggeneration] type CurationResults[Interpretation[_]] =
    EitherT[Interpretation, ProcessingRecoverableError, CuratedTriples[Interpretation]]

  def `INSERT DATA`[TT <: TinyType { type V = String }](resource: String, property: String, value: TT): String =
    s"INSERT DATA { $resource $property '${sparqlEncode(value.value)}'}"

  implicit class JsonOps(json: Json) {

    import io.circe.Decoder.decodeList
    import io.circe.Encoder.encodeList
    import io.circe.optics.JsonPath.root
    import io.circe.{Decoder, Encoder}

    def findTypes: List[String] = {
      val t  = root.`@type`.each.string.getAll(json)
      val tt = root.`@type`.string.getOption(json).toList
      t ++ tt
    }

    def get[T](property: String)(implicit decode: Decoder[T], encode: Encoder[T]): Option[T] =
      root.selectDynamic(property).as[T].getOption(json)

    def getValue[F[_], T](implicit decode: Decoder[T], ME: MonadError[F, Throwable]): OptionT[F, T] =
      singleValueJson(ME).flatMap {
        _.hcursor
          .downField("@value")
          .as[Option[T]]
          .fold(
            fail("No @value property found in Json"),
            OptionT.fromOption[F](_)
          )
      }

    def getId[F[_], T](implicit decode: Decoder[T], ME: MonadError[F, Throwable]): OptionT[F, T] =
      singleValueJson(ME).flatMap {
        _.hcursor
          .downField("@id")
          .as[Option[T]]
          .fold(
            fail("No @value property found in Json"),
            OptionT.fromOption[F](_)
          )
      }

    private def singleValueJson[F[_]](implicit ME: MonadError[F, Throwable]) =
      if (json.isArray) toSingleValue(ME)(json.asArray.map(_.toList))
      else OptionT.some[F](json)

    private def fail[F[_], T](
        message: String
    )(exception: Throwable)(implicit ME: MonadError[F, Throwable]): OptionT[F, T] =
      OptionT.liftF(new IllegalStateException(message, exception).raiseError[F, T])

    private def toSingleValue[F[_], T](implicit ME: MonadError[F, Throwable]): Option[List[T]] => OptionT[F, T] = {
      case Some(v +: Nil) => OptionT.some[F](v)
      case Some(Nil)      => OptionT.none[F, T]
      case None           => OptionT.none[F, T]
      case _              => OptionT.liftF(new IllegalStateException(s"Multiple values found in Json").raiseError[F, T])
    }

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
        property:      Property
    )(implicit decode: Decoder[T], encode: Encoder[T], ME: MonadError[F, Throwable]): OptionT[F, T] =
      getValues(property.toString)(decode, encode) match {
        case Nil      => OptionT.none[F, T]
        case x +: Nil => OptionT.some[F](x)
        case _        => OptionT.liftF(new IllegalStateException(s"Multiple values found for $property").raiseError[F, T])
      }

    def remove(property: Property): Json = root.obj.modify(_.remove(property.toString))(json)
  }

  implicit val entityIdDecoder: Decoder[EntityId] =
    decodeString.emap { value =>
      if (value.trim.isEmpty) "Empty entityId found in the generated triples".asLeft[EntityId]
      else EntityId.of(value).asRight[String]
    }
}
