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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.cli.model.CliParameterValue
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.noDashUuid
import io.renku.graph.model.cli.CliConverters
import io.renku.graph.model.entityModel.LocationLike
import io.renku.graph.model.parameterValues.ValueOverride
import io.renku.graph.model.testentities.StepPlanCommandParameter._
import io.renku.graph.model.testentities.ParameterValue.LocationParameterValue.{CommandInputValue, CommandOutputValue}
import io.renku.graph.model.testentities.ParameterValue._
import io.renku.graph.model.{RenkuUrl, entities, parameterValues}
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import io.renku.tinytypes.constraints.UUID
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

sealed trait ParameterValue extends Product with Serializable {
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

  implicit def toEntitiesParameterValue(implicit renkuUrl: RenkuUrl): ParameterValue => entities.ParameterValue = {
    case p: CommandParameterValue =>
      entities.ParameterValue.CommandParameterValue(
        parameterValues.ResourceId(p.asEntityId.show),
        p.value,
        p.valueReference.to[entities.StepPlanCommandParameter.CommandParameter]
      )
    case p: CommandOutputValue =>
      entities.ParameterValue.CommandOutputValue(parameterValues.ResourceId(p.asEntityId.show),
                                                 p.value,
                                                 p.valueReference.to[entities.StepPlanCommandParameter.CommandOutput]
      )
    case p: CommandInputValue =>
      entities.ParameterValue.CommandInputValue(parameterValues.ResourceId(p.asEntityId.show),
                                                p.value,
                                                p.valueReference.to[entities.StepPlanCommandParameter.CommandInput]
      )
  }

  implicit def toCliParameterValue(implicit renkuUrl: RenkuUrl): ParameterValue => CliParameterValue =
    CliConverters.from(_)

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

  implicit def encoder[PV <: ParameterValue](implicit renkuUrl: RenkuUrl): JsonLDEncoder[PV] =
    JsonLDEncoder.instance(_.to[entities.ParameterValue].asJsonLD)

  implicit def entityIdEncoder[PV <: ParameterValue](implicit renkuUrl: RenkuUrl): EntityIdEncoder[PV] =
    EntityIdEncoder.instance(value => value.activity.asEntityId.asUrlEntityId / "parameter" / value.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID[Id] {
    def generate: Id = noDashUuid.generateAs(Id)
  }
}
