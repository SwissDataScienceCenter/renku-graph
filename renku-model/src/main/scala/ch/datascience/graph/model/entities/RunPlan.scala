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

import ch.datascience.graph.model.InvalidationTime
import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.runPlans._

final case class RunPlan(resourceId:               ResourceId,
                         name:                     Name,
                         maybeDescription:         Option[Description],
                         command:                  Command,
                         maybeProgrammingLanguage: Option[ProgrammingLanguage],
                         keywords:                 List[Keyword],
                         parameters:               List[CommandParameter],
                         inputs:                   List[CommandInput],
                         outputs:                  List[CommandOutput],
                         successCodes:             List[SuccessCode],
                         project:                  Project[ForksCount],
                         maybeInvalidationTime:    Option[InvalidationTime]
)

object RunPlan {
  import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  implicit lazy val encoder: JsonLDEncoder[RunPlan] = JsonLDEncoder.instance { plan =>
    JsonLD.entity(
      plan.resourceId.asEntityId,
      EntityTypes.of(prov / "Plan", renku / "Run"),
      schema / "name"                -> plan.name.asJsonLD,
      schema / "description"         -> plan.maybeDescription.asJsonLD,
      renku / "command"              -> plan.command.asJsonLD,
      schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
      schema / "keywords"            -> plan.keywords.asJsonLD,
      renku / "hasArguments"         -> plan.parameters.asJsonLD,
      renku / "hasInputs"            -> plan.inputs.asJsonLD,
      renku / "hasOutputs"           -> plan.outputs.asJsonLD,
      renku / "successCodes"         -> plan.successCodes.asJsonLD,
      schema / "isPartOf"            -> plan.project.resourceId.asEntityId.asJsonLD,
      prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
    )
  }
}
