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

package ch.datascience.graph.model.entities

import cats.syntax.all._
import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.commandParameters
import ch.datascience.graph.model.commandParameters.Name
import ch.datascience.graph.model.entities.CommandParameterBase._
import ch.datascience.graph.model.entityModel.{Location, LocationLike}
import ch.datascience.graph.model.parameterValues._
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder

sealed trait ParameterValue extends Product with Serializable {
  type ValueReference <: CommandParameterBase
  type Value

  val resourceId:     ResourceId
  val name:           Name
  val value:          Value
  val valueReference: ValueReference
}

object ParameterValue {

  sealed trait LocationParameterValue extends ParameterValue with Product with Serializable {
    override type ValueReference <: CommandInputOrOutput
    override type Value = LocationLike
  }

  final case class CommandParameterValue(resourceId:     ResourceId,
                                         name:           Name,
                                         value:          ValueOverride,
                                         valueReference: CommandParameter
  ) extends ParameterValue {
    type ValueReference = CommandParameter
    type Value          = ValueOverride
  }

  final case class CommandInputValue(resourceId:     ResourceId,
                                     name:           Name,
                                     value:          LocationLike,
                                     valueReference: CommandInput
  ) extends LocationParameterValue {
    type ValueReference = CommandInput
  }

  final case class CommandOutputValue(resourceId:     ResourceId,
                                      name:           Name,
                                      value:          LocationLike,
                                      valueReference: CommandOutput
  ) extends LocationParameterValue {
    type ValueReference = CommandOutput
  }

  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  private val parameterValueTypes = EntityTypes of (renku / "ParameterValue")

  implicit def encoder[PV <: ParameterValue]: JsonLDEncoder[PV] =
    JsonLDEncoder.instance {
      case CommandInputValue(resourceId, name, value, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          parameterValueTypes,
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> value.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case CommandOutputValue(resourceId, name, value, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          parameterValueTypes,
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> value.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case CommandParameterValue(resourceId, name, valueOverride, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          parameterValueTypes,
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> valueOverride.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
    }

  def decoder(plan: Plan): JsonLDDecoder[ParameterValue] = JsonLDDecoder.entity(parameterValueTypes) { cursor =>
    def maybeCommandParameter(resourceId: ResourceId, name: Name, valueReferenceId: commandParameters.ResourceId) = plan
      .findParameter(valueReferenceId)
      .map(parameter =>
        cursor
          .downField(schema / "value")
          .as[ValueOverride]
          .map(value => CommandParameterValue(resourceId, name, value, parameter))
      )

    def maybeCommandInput(resourceId: ResourceId, name: Name, valueReferenceId: commandParameters.ResourceId) =
      plan
        .findInput(valueReferenceId)
        .map(input =>
          cursor
            .downField(schema / "value")
            .as[Location.FileOrFolder]
            .map(value => CommandInputValue(resourceId, name, value, input))
        )

    def maybeCommandOutput(resourceId: ResourceId, name: Name, valueReferenceId: commandParameters.ResourceId) =
      plan
        .findOutput(valueReferenceId)
        .map(output =>
          cursor
            .downField(schema / "value")
            .as[Location.FileOrFolder]
            .map(value => CommandOutputValue(resourceId, name, value, output))
        )

    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      name             <- cursor.downField(schema / "name").as[Name]
      valueReferenceId <- cursor.downField(schema / "valueReference").downEntityId.as[commandParameters.ResourceId]
      parameterValue <-
        List(maybeCommandParameter _, maybeCommandInput _, maybeCommandOutput _)
          .flatMap(_.apply(resourceId, name, valueReferenceId)) match {
          case Nil =>
            DecodingFailure(s"ParameterValue points to a non-existing command parameter $valueReferenceId", Nil).asLeft
          case value :: Nil => value
          case _ =>
            DecodingFailure(s"ParameterValue points to multiple command parameters with $valueReferenceId", Nil).asLeft
        }
    } yield parameterValue
  }
}
