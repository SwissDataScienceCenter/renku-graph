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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.rdfstore.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter, Name}
import ch.datascience.rdfstore.entities.ParameterValue.PathParameterValue.{InputParameterValue, OutputParameterValue}
import ch.datascience.rdfstore.entities.ParameterValue.VariableParameterValue.ValueOverride
import ch.datascience.rdfstore.entities.ParameterValue._
import ch.datascience.tinytypes.constraints.{NonBlank, UUID}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder, _}

sealed trait ParameterValue {
  val id:       Id
  val name:     Name
  val activity: Activity
}

object ParameterValue {

  sealed trait PathParameterValue extends ParameterValue {
    type ValueReference <: CommandParameterBase
    val id:             Id
    val name:           Name
    val valueReference: ValueReference
    val activity:       Activity
  }

  object PathParameterValue {

    def factory(valueReference: CommandInput): Activity => PathParameterValue =
      InputParameterValue(Id.generate, valueReference.name, valueReference, _)

    def factory(valueReference: CommandOutput): Activity => PathParameterValue =
      OutputParameterValue(Id.generate, valueReference.name, valueReference, _)

    final case class InputParameterValue(id: Id, name: Name, valueReference: CommandInput, activity: Activity)
        extends PathParameterValue {
      type ValueReference = CommandInput
    }

    final case class OutputParameterValue(id: Id, name: Name, valueReference: CommandOutput, activity: Activity)
        extends PathParameterValue {
      type ValueReference = CommandOutput
    }
  }

  final case class VariableParameterValue(id:             Id,
                                          name:           Name,
                                          value:          ValueOverride,
                                          valueReference: CommandParameter,
                                          activity:       Activity
  ) extends ParameterValue

  object VariableParameterValue {

    def factory(value: ValueOverride, valueReference: CommandParameter): Activity => VariableParameterValue =
      VariableParameterValue(Id.generate, valueReference.name, value, valueReference, _)

    final class ValueOverride private (val value: String) extends AnyVal with StringTinyType
    implicit object ValueOverride extends TinyTypeFactory[ValueOverride](new ValueOverride(_)) with NonBlank
  }

  implicit def encoder[PV <: ParameterValue](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[PV] =
    JsonLDEncoder.instance {
      case value @ InputParameterValue(_, name, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> valueReference.defaultValue.asJsonLD,
          schema / "valueReference" -> valueReference.asEntityId.asJsonLD
        )
      case value @ OutputParameterValue(_, name, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> valueReference.defaultValue.asJsonLD,
          schema / "valueReference" -> valueReference.asEntityId.asJsonLD
        )
      case value @ VariableParameterValue(_, name, valueOverride, valueReference, _) =>
        JsonLD.entity(
          value.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
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
