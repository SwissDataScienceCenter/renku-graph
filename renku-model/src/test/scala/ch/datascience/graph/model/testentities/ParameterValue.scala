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

package ch.datascience.graph.model.testentities

import cats.syntax.all._
import ch.datascience.graph.model.{RenkuBaseUrl, entities, parameterValues}
import ch.datascience.graph.model.commandParameters.Name
import ch.datascience.graph.model.entityModel.LocationLike
import ch.datascience.graph.model.parameterValues.ValueOverride
import ch.datascience.graph.model.testentities.CommandParameterBase._
import ch.datascience.graph.model.testentities.ParameterValue.PathParameterValue.{InputParameterValue, OutputParameterValue}
import ch.datascience.graph.model.testentities.ParameterValue._
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder, _}

sealed trait ParameterValue {
  type ValueReference <: CommandParameterBase
  val id:             Id
  val name:           Name
  val valueReference: ValueReference
  val activity:       Activity
}

object ParameterValue {

  sealed trait PathParameterValue extends ParameterValue {
    override type ValueReference <: CommandInputOrOutput
    val id:       Id
    val name:     Name
    val location: LocationLike
    val activity: Activity
  }

  implicit lazy val toEntitiesParameterValue: ParameterValue => entities.ParameterValue = {
    case p: VariableParameterValue =>
      entities.ParameterValue.VariableParameterValue(parameterValues.ResourceId(p.asEntityId.show),
                                                     p.name,
                                                     p.value,
                                                     p.valueReference.to[CommandParameter]
      )
    case p: OutputParameterValue =>
      entities.ParameterValue.OutputParameterValue(parameterValues.ResourceId(p.asEntityId.show),
                                                   p.name,
                                                   p.location,
                                                   p.valueReference.to[entities.CommandParameterBase.CommandOutput]
      )
    case p: InputParameterValue =>
      entities.ParameterValue.InputParameterValue(parameterValues.ResourceId(p.asEntityId.show),
                                                  p.name,
                                                  p.location,
                                                  p.valueReference.to[entities.CommandParameterBase.CommandInput]
      )
  }

  object PathParameterValue {

    def factory(location: LocationLike, valueReference: CommandInput): Activity => PathParameterValue =
      InputParameterValue(Id.generate, valueReference.name, location, valueReference, _)

    def factory(location: LocationLike, valueReference: CommandOutput): Activity => PathParameterValue =
      OutputParameterValue(Id.generate, valueReference.name, location, valueReference, _)

    final case class InputParameterValue(id:             Id,
                                         name:           Name,
                                         location:       LocationLike,
                                         valueReference: CommandInput,
                                         activity:       Activity
    ) extends PathParameterValue {
      type ValueReference = CommandInput
    }

    final case class OutputParameterValue(id:             Id,
                                          name:           Name,
                                          location:       LocationLike,
                                          valueReference: CommandOutput,
                                          activity:       Activity
    ) extends PathParameterValue {
      type ValueReference = CommandOutput
    }
  }

  final case class VariableParameterValue(id:             Id,
                                          name:           Name,
                                          value:          ValueOverride,
                                          valueReference: CommandParameter,
                                          activity:       Activity
  ) extends ParameterValue {
    type ValueReference = CommandParameter
  }

  object VariableParameterValue {

    def factory(value: ValueOverride, valueReference: CommandParameter): Activity => VariableParameterValue =
      VariableParameterValue(Id.generate, valueReference.name, value, valueReference, _)

  }

  implicit def encoder[PV <: ParameterValue](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[PV] =
    JsonLDEncoder.instance {
      case value @ InputParameterValue(_, name, location, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.asEntityId.asJsonLD
        )
      case value @ OutputParameterValue(_, name, location, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.asEntityId.asJsonLD
        )
      case value @ VariableParameterValue(_, name, valueOverride, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "VariableParameterValue"),
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> valueOverride.asJsonLD,
          schema / "valueReference" -> valueReference.asEntityId.asJsonLD
        )
    }

  implicit def entityIdEncoder[PV <: ParameterValue](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[PV] =
    EntityIdEncoder.instance(value => value.activity.asEntityId.asUrlEntityId / "parameter" / value.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }
}
