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
import ch.datascience.graph.model.entityModel.LocationLike
import ch.datascience.graph.model.parameterValues.ValueOverride
import ch.datascience.graph.model.testentities.CommandParameterBase._
import ch.datascience.graph.model.testentities.ParameterValue.LocationParameterValue.{CommandInputValue, CommandOutputValue}
import ch.datascience.graph.model.testentities.ParameterValue._
import ch.datascience.graph.model.{RenkuBaseUrl, entities, parameterValues}
import ch.datascience.tinytypes.constraints.UUID
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait ParameterValue {
  type ValueReference <: CommandParameterBase
  type Value

  val id:             Id
  val valueReference: ValueReference
  val value:          Value
  val activity:       Activity
}

object ParameterValue {

  sealed trait LocationParameterValue extends ParameterValue {
    override type ValueReference <: CommandInputOrOutput
    override type Value = LocationLike
  }

  implicit def toEntitiesParameterValue(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): ParameterValue => entities.ParameterValue = {
    case p: CommandParameterValue =>
      entities.ParameterValue.CommandParameterValue(parameterValues.ResourceId(p.asEntityId.show),
                                                    p.value,
                                                    p.valueReference.to[entities.CommandParameterBase.CommandParameter]
      )
    case p: CommandOutputValue =>
      entities.ParameterValue.CommandOutputValue(parameterValues.ResourceId(p.asEntityId.show),
                                                 p.value,
                                                 p.valueReference.to[entities.CommandParameterBase.CommandOutput]
      )
    case p: CommandInputValue =>
      entities.ParameterValue.CommandInputValue(parameterValues.ResourceId(p.asEntityId.show),
                                                p.value,
                                                p.valueReference.to[entities.CommandParameterBase.CommandInput]
      )
  }

  object LocationParameterValue {

    def factory(location: LocationLike, valueReference: CommandInput): Activity => LocationParameterValue =
      CommandInputValue(Id.generate, location, valueReference, _)

    def factory(location: LocationLike, valueReference: CommandOutput): Activity => LocationParameterValue =
      CommandOutputValue(Id.generate, location, valueReference, _)

    final case class CommandInputValue(id: Id, value: LocationLike, valueReference: CommandInput, activity: Activity)
        extends LocationParameterValue {
      type ValueReference = CommandInput
    }

    final case class CommandOutputValue(id: Id, value: LocationLike, valueReference: CommandOutput, activity: Activity)
        extends LocationParameterValue {
      type ValueReference = CommandOutput
    }
  }

  final case class CommandParameterValue(id:             Id,
                                         value:          ValueOverride,
                                         valueReference: CommandParameter,
                                         activity:       Activity
  ) extends ParameterValue {
    type ValueReference = CommandParameter
    override type Value = ValueOverride
  }

  object CommandParameterValue {

    def factory(value: ValueOverride, valueReference: CommandParameter): Activity => CommandParameterValue =
      CommandParameterValue(Id.generate, value, valueReference, _)
  }

  implicit def encoder[PV <: ParameterValue](implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[PV] =
    JsonLDEncoder.instance(_.to[entities.ParameterValue].asJsonLD)

  implicit def entityIdEncoder[PV <: ParameterValue](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[PV] =
    EntityIdEncoder.instance(value => value.activity.asEntityId.asUrlEntityId / "parameter" / value.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id(java.util.UUID.randomUUID.toString)
  }
}
