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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.commandParameters._
import io.renku.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.ontology._
import io.renku.jsonld.syntax._
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

/** A parameter for a [[CompositePlan]] that is mapping onto parameters of its enclosing plans. */
final case class ParameterMapping(
    resourceId:       ResourceId,
    defaultValue:     ParameterMapping.DefaultValue,
    maybeDescription: Option[Description],
    name:             Name,
    mappedParameter:  NonEmptyList[ResourceId]
)

object ParameterMapping {
  final case class DefaultValue(value: String) extends StringTinyType

  object DefaultValue extends TinyTypeFactory[DefaultValue](new DefaultValue(_)) {
    implicit val jsonEncoder: JsonLDEncoder[DefaultValue] =
      JsonLDEncoder.encodeString.contramap(_.value)
  }

  val entityTypes: EntityTypes = EntityTypes.of(renku / "ParameterMapping", renku / "CommandParameterBase")
  val ontology: Type = {
    val mappingClass = Class(renku / "ParameterMapping")
    Type.Def(
      mappingClass,
      ObjectProperties(
        ObjectProperty(
          renku / "mapsTo",
          mappingClass,
          CommandInput.ontology.clazz,
          CommandOutput.ontology.clazz,
          CommandParameter.ontology.clazz
        )
      ),
      DataProperties(
        DataProperty(renku / "name", xsd / "string"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(schema / "defaultValue", xsd / "string")
      )
    )
  }

  implicit def encoder: JsonLDEncoder[ParameterMapping] =
    JsonLDEncoder.instance { mapping =>
      JsonLD.entity(
        mapping.resourceId.asEntityId,
        entityTypes,
        schema / "name"         -> mapping.name.asJsonLD,
        schema / "description"  -> mapping.maybeDescription.asJsonLD,
        schema / "defaultValue" -> mapping.defaultValue.asJsonLD,
        renku / "mapsTo"        -> mapping.mappedParameter.toList.asJsonLD
      )
    }
}
