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

import cats.syntax.show._
import CliConversionFunctions._
import io.renku.cli.model._
import io.renku.graph.model.{RenkuUrl, activities, agents, associations, commandParameters, entities, generations, parameterValues, testentities, usages}
import io.renku.graph.model.parameterValues.ValueOverride
import io.renku.jsonld.syntax._

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

  def from(a: testentities.Activity)(implicit renkuUrl: RenkuUrl): CliActivity = CliActivity(
    activities.ResourceId(a.asEntityId.show),
    a.startTime,
    a.endTime,
    CliAgent.Software(from(a.agent)),
    CliAgent.Person(from(a.author)),
    from(a.association),
    a.usages.map(from),
    a.generations.map(from),
    a.parameters.map(from)
  )

  def from(a: entities.Agent): CliSoftwareAgent = CliSoftwareAgent(a.resourceId, a.name)

  def from(a: testentities.Agent)(implicit renkuUrl: RenkuUrl): CliSoftwareAgent =
    CliSoftwareAgent(agents.ResourceId(a.asEntityId.show), agents.Name(s"renku ${a.cliVersion}"))

  def from(association: entities.Association, allPlans: List[entities.Plan]): CliAssociation = {
    val stepPlan       = findStepPlanOrFail(association.planId, allPlans)
    val associatedPlan = CliAssociation.AssociatedPlan(from(stepPlan))
    association
      .fold { a =>
        CliAssociation(a.resourceId, CliAgent(from(a.agent)), associatedPlan)
      } { a =>
        CliAssociation(a.resourceId, CliAgent(from(a.agent)), associatedPlan)
      }
  }

  def from(association: testentities.Association)(implicit renkuUrl: RenkuUrl): CliAssociation = {
    val associatedPlan = CliAssociation.AssociatedPlan(from(association.plan))
    val agent = association.agentOrPerson.fold(
      a => CliAgent(from(a)),
      a => CliAgent(from(a))
    )
    val id = associations.ResourceId(association.asEntityId.show)
    CliAssociation(id, agent, associatedPlan)
  }

  def from(usage: entities.Usage): CliUsage = CliUsage(
    usage.resourceId,
    from(usage.entity)
  )

  def from(usage: testentities.Usage)(implicit renkuUrl: RenkuUrl): CliUsage = CliUsage(
    usages.ResourceId(usage.asEntityId.show),
    from(usage.entity)
  )

  def from(generation: entities.Generation): CliGeneration = CliGeneration(
    generation.resourceId,
    from(generation.entity),
    generation.activityResourceId
  )

  def from(generation: testentities.Generation)(implicit renkuUrl: RenkuUrl): CliGeneration = CliGeneration(
    generations.ResourceId(generation.asEntityId.show),
    from(generation.entity),
    activities.ResourceId(generation.activity.asEntityId.show)
  )

  def from(paramValue: entities.ParameterValue): CliParameterValue = paramValue match {
    case v: entities.ParameterValue.LocationParameterValue =>
      CliParameterValue(v.resourceId, v.valueReference.resourceId, ValueOverride(v.value.value))
    case v: entities.ParameterValue.CommandParameterValue =>
      CliParameterValue(v.resourceId, v.valueReference.resourceId, v.value)
  }

  def from(paramValue: testentities.ParameterValue)(implicit renkuUrl: RenkuUrl): CliParameterValue = {
    val id = parameterValues.ResourceId(paramValue.asEntityId.show)
    paramValue match {
      case p: testentities.ParameterValue.LocationParameterValue.CommandOutputValue =>
        CliParameterValue(id,
                          commandParameters.ResourceId(p.valueReference.asEntityId.show),
                          ValueOverride(p.value.value)
        )
      case p: testentities.ParameterValue.LocationParameterValue.CommandInputValue =>
        CliParameterValue(id,
                          commandParameters.ResourceId(p.valueReference.asEntityId.show),
                          ValueOverride(p.value.value)
        )
      case p: testentities.ParameterValue.CommandParameterValue =>
        CliParameterValue(id, commandParameters.ResourceId(p.valueReference.asEntityId.show), p.value)
    }
  }
}

object CliActivityConverters extends CliActivityConverters
