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

import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.commandParameters.Name
import ch.datascience.graph.model.entities.CommandParameterBase._
import ch.datascience.graph.model.entityModel.Location
import ch.datascience.graph.model.parameterValues._

sealed trait ParameterValue {
  type ValueReference <: CommandParameterBase
  val resourceId:     ResourceId
  val name:           Name
  val valueReference: ValueReference
}

object ParameterValue {

  sealed trait PathParameterValue extends ParameterValue {
    override type ValueReference <: CommandInputOrOutput
    val resourceId: ResourceId
    val name:       Name
    val location:   Location
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
                                       location:       Location,
                                       valueReference: CommandInput
  ) extends PathParameterValue {
    type ValueReference = CommandInput
  }

  final case class OutputParameterValue(resourceId:     ResourceId,
                                        name:           Name,
                                        location:       Location,
                                        valueReference: CommandOutput
  ) extends PathParameterValue {
    type ValueReference = CommandOutput
  }

  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  implicit def encoder[PV <: ParameterValue]: JsonLDEncoder[PV] =
    JsonLDEncoder.instance {
      case InputParameterValue(resourceId, name, location, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case OutputParameterValue(resourceId, name, location, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          prov / "atLocation"       -> location.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
      case VariableParameterValue(resourceId, name, valueOverride, valueReference) =>
        JsonLD.entity(
          resourceId.asEntityId,
          EntityTypes of (renku / "ParameterValue", renku / "PathParameterValue"),
          schema / "name"           -> name.asJsonLD,
          schema / "value"          -> valueOverride.asJsonLD,
          schema / "valueReference" -> valueReference.resourceId.asEntityId.asJsonLD
        )
    }

}
