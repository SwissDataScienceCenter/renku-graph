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
import io.renku.jsonld.{JsonLDDecoder, JsonLDEntityDecoder}

sealed trait ParameterValue {
  type ValueReference <: CommandParameterBase
  val resourceId:     ResourceId
  val name:           Name
  val valueReference: ValueReference
}

object ParameterValue {

  sealed trait PathParameterValue extends ParameterValue with Product with Serializable {
    override type ValueReference <: CommandInputOrOutput
    val resourceId: ResourceId
    val name:       Name
    val location:   LocationLike
  }

  final case class VariableParameterValue(resourceId:     ResourceId,
                                          name:           Name,
                                          value:          ValueOverride,
                                          valueReference: CommandParameter
  ) extends ParameterValue {
    type ValueReference = CommandParameter
  }

  final case class InputParameterValue(resourceId:     ResourceId,
                                       name:           Name,
                                       location:       LocationLike,
                                       valueReference: CommandInput
  ) extends PathParameterValue {
    type ValueReference = CommandInput
  }

  final case class OutputParameterValue(resourceId:     ResourceId,
                                        name:           Name,
                                        location:       LocationLike,
                                        valueReference: CommandOutput
  ) extends PathParameterValue {
    type ValueReference = CommandOutput
  }

  import io.renku.jsonld.JsonLDEncoder._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  private val pathParameterTypes     = EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue")
  private val variableParameterTypes = EntityTypes of (renku / "ParameterValue", renku / "VariableParameterValue")

  implicit def encoder[PV <: ParameterValue]: JsonLDEncoder[PV] =
    JsonLDEncoder.instance {
      case InputParameterValue(resourceId, name, location, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          pathParameterTypes,
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case OutputParameterValue(resourceId, name, location, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          pathParameterTypes,
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case VariableParameterValue(resourceId, name, valueOverride, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          variableParameterTypes,
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> valueOverride.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
    }

  def decoder(plan: Plan): JsonLDDecoder[ParameterValue] =
    variableParameterDecoder(plan)
      .orElse(pathParameterDecoder(plan).widen[ParameterValue])

  private def pathParameterDecoder(plan: Plan): JsonLDEntityDecoder[PathParameterValue] =
    JsonLDDecoder.entity(pathParameterTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        name             <- cursor.downField(schema / "name").as[Name]
        location         <- cursor.downField(prov / "atLocation").as[Location.FileOrFolder]
        valueReferenceId <- cursor.downField(schema / "valueReference").downEntityId.as[commandParameters.ResourceId]
        parameterValue <-
          Either.fromOption(
            plan
              .findInput(valueReferenceId)
              .map(InputParameterValue(resourceId, name, location, _))
              .orElse(
                plan
                  .findOutput(valueReferenceId)
                  .map(OutputParameterValue(resourceId, name, location, _))
              ),
            DecodingFailure(s"PathParameterValue points to a non-existing command parameter $valueReferenceId", Nil)
          )
      } yield parameterValue
    }

  private def variableParameterDecoder(plan: Plan): JsonLDEntityDecoder[VariableParameterValue] =
    JsonLDDecoder.entity(variableParameterTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[ResourceId]
        name             <- cursor.downField(schema / "name").as[Name]
        valueOverride    <- cursor.downField(schema / "value").as[ValueOverride]
        valueReferenceId <- cursor.downField(schema / "valueReference").downEntityId.as[commandParameters.ResourceId]
        commandParameter <-
          Either.fromOption(
            plan.findParameter(valueReferenceId),
            DecodingFailure(s"VariableParameterValue points to a non-existing command parameter $valueReferenceId", Nil)
          )
      } yield VariableParameterValue(resourceId, name, valueOverride, commandParameter)
    }
}
