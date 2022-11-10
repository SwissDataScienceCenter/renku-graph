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
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.commandParameters._
import io.renku.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}
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

  object DefaultValue extends TinyTypeFactory[DefaultValue](new DefaultValue(_)) with TinyTypeJsonLDOps[DefaultValue]

  // noinspection TypeAnnotation
  object Ontology {
    val entityTypes: EntityTypes = EntityTypes.of(renku / "ParameterMapping", renku / "CommandParameterBase")

    val parameterMappingClass = Class(renku / "ParameterMapping", ParentClass(renku / "CommandParameterBase"))

    val mapsTo       = renku / "mapsTo"
    val name         = renku / "name"
    val description  = schema / "description"
    val defaultValue = schema / "defaultValue"

    val typeDef: Type =
      Type.Def(
        parameterMappingClass,
        ObjectProperties(
          ObjectProperty(
            mapsTo,
            parameterMappingClass,
            CommandInput.ontology.clazz,
            CommandOutput.ontology.clazz,
            CommandParameter.ontology.clazz
          )
        ),
        DataProperties(
          DataProperty(name, xsd / "string"),
          DataProperty(description, xsd / "string"),
          DataProperty(defaultValue, xsd / "string")
        )
      )
  }

  implicit def encoder: JsonLDEncoder[ParameterMapping] =
    JsonLDEncoder.instance { mapping =>
      JsonLD.entity(
        mapping.resourceId.asEntityId,
        Ontology.entityTypes,
        Ontology.name         -> mapping.name.asJsonLD,
        Ontology.description  -> mapping.maybeDescription.asJsonLD,
        Ontology.defaultValue -> mapping.defaultValue.asJsonLD,
        Ontology.mapsTo       -> mapping.mappedParameter.toList.asJsonLD
      )
    }

  implicit def decoder: JsonLDDecoder[ParameterMapping] =
    JsonLDDecoder.instance { cursor =>
      for {
        id          <- cursor.downEntityId.as[ResourceId]
        name        <- cursor.downField(Ontology.name).as[Name]
        defaultVal  <- cursor.downField(Ontology.defaultValue).as[DefaultValue]
        descr       <- cursor.downField(Ontology.description).as[Option[Description]]
        mappedParam <- cursor.downField(Ontology.mapsTo).as[NonEmptyList[ResourceId]]
      } yield ParameterMapping(id, defaultVal, descr, name, mappedParam)
    }
}
