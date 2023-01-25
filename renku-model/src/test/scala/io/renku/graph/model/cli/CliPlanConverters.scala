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
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.cli.model.CliMappedIOStream.StreamType
import io.renku.cli.model._
import io.renku.graph.model.{commandParameters, entities, plans}

trait CliPlanConverters extends CliCommonConverters {

  def from(plan: entities.Plan): CliPlan = plan.fold(
    from(_: entities.StepPlan),
    from(_: entities.StepPlan),
    from(_: entities.CompositePlan),
    from(_: entities.CompositePlan)
  )

  def from(plan: entities.StepPlan): CliStepPlan = plan match {
    case p: entities.StepPlan.NonModified =>
      CliStepPlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        dateModified = None,
        p.keywords,
        p.maybeCommand,
        p.parameters.map(from),
        p.inputs.map(from),
        p.outputs.map(from),
        p.successCodes,
        derivedFrom = None,
        invalidationTime = None
      )
    case p: entities.StepPlan.Modified =>
      CliStepPlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        plans.DateModified(p.dateCreated.value).some,
        p.keywords,
        p.maybeCommand,
        p.parameters.map(from),
        p.inputs.map(from),
        p.outputs.map(from),
        p.successCodes,
        derivedFrom = None,
        invalidationTime = None
      )
  }

  def from(plan: entities.CompositePlan, allPlans: List[entities.Plan]): CliCompositePlan = plan match {
    case p: entities.CompositePlan.NonModified =>
      CliCompositePlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        dateModified = None,
        p.keywords,
        derivedFrom = None,
        invalidationTime = None,
        collectAllPlans(p.plans, allPlans).map(from),
        p.links.map(from),
        p.mappings.map(from)
      )
    case p: entities.CompositePlan.Modified =>
      CliCompositePlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        dateModified = plans.DateModified(p.dateCreated.value).some,
        p.keywords,
        derivedFrom = None,
        invalidationTime = None,
        collectAllPlans(p.plans, allPlans).map(from),
        p.links.map(from),
        p.mappings.map(from)
      )
  }

  def from(cp: entities.StepPlanCommandParameter.CommandParameter): CliCommandParameter = cp match {
    case cp: entities.StepPlanCommandParameter.ExplicitCommandParameter =>
      CliCommandParameter(cp.resourceId,
                          cp.name,
                          cp.maybeDescription,
                          cp.maybePrefix,
                          cp.position.some,
                          cp.defaultValue
      )
    case cp: entities.StepPlanCommandParameter.ImplicitCommandParameter =>
      CliCommandParameter(cp.resourceId, cp.name, cp.maybeDescription, cp.maybePrefix, position = None, cp.defaultValue)
  }

  def from(cp: entities.StepPlanCommandParameter.CommandInput): CliCommandInput = cp match {
    case cp: entities.StepPlanCommandParameter.LocationCommandInput =>
      CliCommandInput(
        cp.resourceId,
        cp.name,
        cp.maybeDescription,
        cp.maybePrefix,
        cp.position.some,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        mappedTo = None,
        cp.maybeEncodingFormat
      )
    case cp: entities.StepPlanCommandParameter.MappedCommandInput =>
      CliCommandInput(
        cp.resourceId,
        cp.name,
        cp.maybeDescription,
        cp.maybePrefix,
        cp.position.some,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        from(cp.mappedTo).some,
        cp.maybeEncodingFormat
      )
    case cp: entities.StepPlanCommandParameter.ImplicitCommandInput =>
      CliCommandInput(
        cp.resourceId,
        cp.name,
        description = None,
        cp.maybePrefix,
        position = None,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        mappedTo = None,
        cp.maybeEncodingFormat
      )
  }

  def from(cp: entities.StepPlanCommandParameter.CommandOutput): CliCommandOutput = cp match {
    case cp: entities.StepPlanCommandParameter.LocationCommandOutput =>
      CliCommandOutput(
        cp.resourceId,
        cp.name,
        cp.maybeDescription,
        cp.maybePrefix,
        cp.position.some,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        mappedTo = None,
        cp.maybeEncodingFormat,
        cp.folderCreation
      )
    case cp: entities.StepPlanCommandParameter.MappedCommandOutput =>
      CliCommandOutput(
        cp.resourceId,
        cp.name,
        cp.maybeDescription,
        cp.maybePrefix,
        cp.position.some,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        from(cp.mappedTo).some,
        cp.maybeEncodingFormat,
        cp.folderCreation
      )
    case cp: entities.StepPlanCommandParameter.ImplicitCommandOutput =>
      CliCommandOutput(
        cp.resourceId,
        cp.name,
        description = None,
        cp.maybePrefix,
        position = None,
        commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
        mappedTo = None,
        cp.maybeEncodingFormat,
        cp.folderCreation
      )
  }

  def from(ioStream: commandParameters.IOStream): CliMappedIOStream = CliMappedIOStream(
    ioStream.resourceId,
    StreamType.fromString(ioStream.name.value).fold(err => throw new Exception(err), identity)
  )

  def from(pl: entities.ParameterLink, plans: List[entities.Plan]): CliParameterLink =
    CliParameterLink(
      pl.resourceId,
      from(findCommandOutput(pl.source, plans)),
      buildSinks(pl.sinks, plans)
    )

  private def buildSinks(sinkIds: NonEmptyList[commandParameters.ResourceId], plans: List[entities.Plan]) = {
    val paramSinks = collectAllParameters(sinkIds, plans).map(from).map(CliParameterLink.Sink.apply)
    val inputSinks = collectAllInputParameters(sinkIds, plans).map(from).map(CliParameterLink.Sink.apply)

    paramSinks ::: inputSinks match {
      case s if s.size == sinkIds.size => NonEmptyList.fromListUnsafe(s)
      case _                           => throw new Exception("Cannot find all ParameterLink sinks")
    }
  }

  def from(mapping: entities.ParameterMapping): CliParameterMapping =
    CliParameterMapping(
      mapping.resourceId,
      mapping.name,
      mapping.maybeDescription,
      mapping.maybePrefix,
      position = None,
      commandParameters.ParameterDefaultValue(mapping.defaultValue.value).some,
      mapping.mappedParameter
    )
}

object CliPlanConverters extends CliPlanConverters
