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

package io.renku.cli.model

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.cli.model.Ontologies.Renku
import io.renku.graph.model.commandParameters.IOStream._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliMappedIOStream(
    id:         ResourceId,
    streamType: CliMappedIOStream.StreamType
) extends CliModel

object CliMappedIOStream {

  sealed trait StreamType extends Product {
    final val name = this.productPrefix.toLowerCase
  }
  object StreamType {
    case object StdIn  extends StreamType
    case object StdOut extends StreamType
    case object StdErr extends StreamType

    val all: NonEmptyList[StreamType] = NonEmptyList.of(StdIn, StdOut, StdErr)

    def fromString(str: String): Either[String, StreamType] =
      all.find(_.name.equalsIgnoreCase(str)).toRight(s"Invalid stream type: $str. Expected one of: $all")

    implicit val jsonLDEncoder: JsonLDEncoder[StreamType] =
      JsonLDEncoder.encodeString.contramap(_.name)

    implicit val jsonLDDecoder: JsonLDDecoder[StreamType] =
      JsonLDDecoder.decodeString.emap(fromString)
  }

  private val entityTypes = EntityTypes.of(Renku.IOStream)

  implicit val jsonLDDecoder: JsonLDDecoder[CliMappedIOStream] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        stream     <- cursor.downField(Renku.streamType).as[StreamType]
      } yield CliMappedIOStream(resourceId, stream)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliMappedIOStream] =
    JsonLDEncoder.instance { value =>
      JsonLD.entity(value.id.asEntityId, entityTypes, Renku.streamType -> value.streamType.asJsonLD)
    }
}
