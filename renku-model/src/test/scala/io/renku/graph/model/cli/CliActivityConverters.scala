/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.cli

import CliConversionFunctions._
import io.renku.cli.model._
import io.renku.graph.model.entities
import io.renku.graph.model.parameterValues.ValueOverride

trait CliActivityConverters extends CliPlanConverters {

  def from(a: entities.Activity, plans: List[entities.Plan]): CliActivity = CliActivity(
    a.resourceId,
    a.startTime,
    a.endTime,
    CliAgent.Software(from(a.agent)),
    CliAgent.Person(from(a.author)),
    from(a.association, plans),
    a.usages.map(from),
    a.generations.map(from),
    a.parameters.map(from)
  )

  def from(a: entities.Agent): CliSoftwareAgent = CliSoftwareAgent(a.resourceId, a.name)

  def from(association: entities.Association, plans: List[entities.Plan]): CliAssociation = {
    val associatedPlan = CliAssociation.AssociatedPlan(from(findStepPlanOrFail(association.planId, plans)))
    association
      .fold { a =>
        CliAssociation(a.resourceId, CliAgent(from(a.agent)), associatedPlan)
      } { a =>
        CliAssociation(a.resourceId, CliAgent(from(a.agent)), associatedPlan)
      }
  }

  def from(usage: entities.Usage): CliUsage = CliUsage(
    usage.resourceId,
    from(usage.entity)
  )

  def from(generation: entities.Generation): CliGeneration = CliGeneration(
    generation.resourceId,
    from(generation.entity),
    generation.activityResourceId
  )

  def from(paramValue: entities.ParameterValue): CliParameterValue = paramValue match {
    case v: entities.ParameterValue.LocationParameterValue =>
      CliParameterValue(v.resourceId, v.valueReference.resourceId, ValueOverride(v.value.value))
    case v: entities.ParameterValue.CommandParameterValue =>
      CliParameterValue(v.resourceId, v.valueReference.resourceId, v.value)
  }
}

object CliActivityConverters extends CliActivityConverters
