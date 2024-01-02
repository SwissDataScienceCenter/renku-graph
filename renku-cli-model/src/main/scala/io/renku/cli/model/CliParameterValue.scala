/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import io.renku.cli.model.Ontologies.{Renku, Schema}
import io.renku.graph.model.commandParameters
import io.renku.graph.model.parameterValues._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliParameterValue(
    id:        ResourceId,
    parameter: commandParameters.ResourceId,
    value:     CliParameterValue.Value
) extends CliModel

object CliParameterValue {
  private val entityTypes = EntityTypes.of(Schema.PropertyValue, Renku.ParameterValue)

  final case class Value(json: JsonLD) {
    def asString: String =
      json.cursor
        .as[String]
        .orElse(json.cursor.as[Boolean].map(_.toString))
        .orElse(json.cursor.as[BigDecimal].map(_.bigDecimal.toString))
        .getOrElse(json.toJson.noSpaces)
  }
  object Value {
    def apply(str:  String):  Value = Value(JsonLD.fromString(str))
    def apply(bool: Boolean): Value = Value(JsonLD.fromBoolean(bool))
    def apply(n:    Long):    Value = Value(JsonLD.fromLong(n))

    implicit val jsonLDDecoder: JsonLDDecoder[Value] =
      JsonLDDecoder.instance(c => Right(Value(c.jsonLD)))

    implicit val jsonLDEncoder: JsonLDEncoder[Value] =
      JsonLDEncoder.instance(_.json)
  }

  implicit val jsonLDDecoder: JsonLDDecoder[CliParameterValue] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        valueReferenceId <- cursor.downField(Schema.valueReference).downEntityId.as[commandParameters.ResourceId]
        parameterValue   <- cursor.downField(Schema.value).as[Value]
      } yield CliParameterValue(resourceId, valueReferenceId, parameterValue)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliParameterValue] =
    JsonLDEncoder.instance { pv =>
      JsonLD.entity(
        pv.id.asEntityId,
        entityTypes,
        Schema.valueReference -> pv.parameter.asJsonLD,
        Schema.value          -> pv.value.asJsonLD
      )
    }
}
