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

  def from(plan: entities.Plan, allPlans: List[entities.Plan]): CliPlan = plan.fold(
    spnm => CliPlan(from(spnm)),
    spm => CliPlan(from(spm)),
    cpnm => CliPlan(from(cpnm, allPlans)),
    cpm => CliPlan(from(cpm, allPlans))
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
        derivedFrom = p.derivation.derivedFrom.some,
        invalidationTime = p.maybeInvalidationTime
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
        collectAllPlans(p.plans, allPlans).map(from(_, allPlans)),
        p.links.map(from(_, allPlans)),
        p.mappings.map(from(_, allPlans))
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
        derivedFrom = p.derivation.derivedFrom.some,
        invalidationTime = p.maybeInvalidationTime,
        collectAllPlans(p.plans, allPlans).map(from(_, allPlans)),
        p.links.map(from(_, allPlans)),
        p.mappings.map(from(_, allPlans))
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

  def from(pl: entities.ParameterLink, allPlans: List[entities.Plan]): CliParameterLink =
    CliParameterLink(
      pl.resourceId,
      from(findCommandOutput(pl.source, allPlans)),
      buildSinks(pl.sinks, allPlans)
    )

  private def buildSinks(sinkIds: NonEmptyList[commandParameters.ResourceId], allPlans: List[entities.Plan]) = {
    val paramSinks = collectAllParameters(sinkIds, allPlans).map(from).map(CliParameterLink.Sink.apply)
    val inputSinks = collectAllInputParameters(sinkIds, allPlans).map(from).map(CliParameterLink.Sink.apply)

    paramSinks ::: inputSinks match {
      case s if s.size == sinkIds.size => NonEmptyList.fromListUnsafe(s)
      case _                           => throw new Exception("Cannot find all ParameterLink sinks")
    }
  }

  def from(mapping: entities.ParameterMapping, allPlans: List[entities.Plan]): CliParameterMapping =
    CliParameterMapping(
      mapping.resourceId,
      mapping.name,
      mapping.maybeDescription,
      mapping.maybePrefix,
      position = None,
      mapping.defaultValue.map(v => commandParameters.ParameterDefaultValue(v.value)),
      buildMappedParams(mapping.mappedParameter, allPlans)
    )

  private def buildMappedParams(paramIds: NonEmptyList[commandParameters.ResourceId], allPlans: List[entities.Plan]) = {
    val params  = collectAllParameters(paramIds, allPlans).map(from).map(CliParameterMapping.MappedParam.apply)
    val inputs  = collectAllInputParameters(paramIds, allPlans).map(from).map(CliParameterMapping.MappedParam.apply)
    val outputs = collectAllOutputParameters(paramIds, allPlans).map(from).map(CliParameterMapping.MappedParam.apply)
    val mappings =
      collectAllParameterMappings(paramIds, allPlans).map(from(_, allPlans)).map(CliParameterMapping.MappedParam.apply)

    params ::: inputs ::: outputs ::: mappings match {
      case s if s.size == paramIds.size => NonEmptyList.fromListUnsafe(s)
      case s =>
        throw new Exception(s"Cannot find all mapsTo ParameterMappings: found ${s.size}, expected ${paramIds.size}")
    }
  }
}

object CliPlanConverters extends CliPlanConverters
