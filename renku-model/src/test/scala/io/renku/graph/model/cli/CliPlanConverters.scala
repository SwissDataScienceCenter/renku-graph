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
import io.renku.graph.model.testentities.HavingInvalidationTime
import io.renku.graph.model.{RenkuUrl, commandParameters, entities, parameterLinks, plans, testentities}
import io.renku.jsonld.syntax._

trait CliPlanConverters extends CliCommonConverters {

  def from(plan: entities.Plan, allPlans: List[entities.Plan]): CliPlan = plan.fold(
    spnm => CliPlan(from(spnm)),
    spm => CliPlan(from(spm)),
    cpnm => CliPlan(from(cpnm, allPlans)),
    cpm => CliPlan(from(cpm, allPlans))
  )

  def from(plan: testentities.Plan)(implicit renkuUrl: RenkuUrl): CliPlan = plan.fold(
    p => CliPlan(from(p)),
    p => CliPlan(from(p)),
    p => CliPlan(from(p)),
    p => CliPlan(from(p))
  )

  def from(plan: entities.StepPlan): CliStepPlan = plan match {
    case p: entities.StepPlan.NonModified =>
      CliStepPlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        dateModified = plans.DateModified(p.dateCreated.value),
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
        plans.DateModified(p.dateCreated.value),
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

  def from(plan: testentities.StepPlan)(implicit renkuUrl: RenkuUrl): CliStepPlan =
    CliStepPlan(
      plans.ResourceId(plan.asEntityId.show),
      plan.name,
      plan.maybeDescription,
      plan.creators.map(from),
      plan.dateCreated,
      plans.DateModified(plan.dateCreated.value),
      plan.keywords,
      plan.maybeCommand,
      plan.parameters.map(from),
      plan.inputs.map(from),
      plan.outputs.map(from),
      plan.successCodes,
      derivedFrom = plan match {
        case p: testentities.StepPlan.Modified =>
          plans.DerivedFrom(p.parent.asEntityId).some
        case _ => None
      },
      invalidationTime = plan match {
        case p: HavingInvalidationTime =>
          p.invalidationTime.some
        case _ => None
      }
    )

  def from(plan: entities.CompositePlan, allPlans: List[entities.Plan]): CliCompositePlan = plan match {
    case p: entities.CompositePlan.NonModified =>
      CliCompositePlan(
        p.resourceId,
        p.name,
        p.maybeDescription,
        p.creators.map(from),
        p.dateCreated,
        dateModified = plans.DateModified(p.dateCreated.value),
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
        dateModified = plans.DateModified(p.dateCreated.value),
        p.keywords,
        derivedFrom = p.derivation.derivedFrom.some,
        invalidationTime = p.maybeInvalidationTime,
        collectAllPlans(p.plans, allPlans).map(from(_, allPlans)),
        p.links.map(from(_, allPlans)),
        p.mappings.map(from(_, allPlans))
      )
  }

  def from(plan: testentities.CompositePlan)(implicit renkuUrl: RenkuUrl): CliCompositePlan =
    CliCompositePlan(
      plans.ResourceId(plan.asEntityId.show),
      plan.name,
      plan.maybeDescription,
      plan.creators.map(from),
      plan.dateCreated,
      dateModified = plans.DateModified(plan.dateCreated.value),
      plan.keywords,
      derivedFrom = plan match {
        case p: testentities.CompositePlan.Modified =>
          plans.DerivedFrom(p.parent.asEntityId).some
        case _ => None
      },
      invalidationTime = plan match {
        case p: testentities.HavingInvalidationTime =>
          p.invalidationTime.some
        case _ => None
      },
      plan.plans.map(from),
      plan.links.map(from(_)),
      plan.mappings.map(from(_))
    )

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

  def from(
      cp: testentities.StepPlanCommandParameter.CommandParameter
  )(implicit renkuUrl: RenkuUrl): CliCommandParameter =
    CliCommandParameter(
      commandParameters.ResourceId(cp.asEntityId.show),
      cp.name,
      cp.maybeDescription,
      cp.maybePrefix,
      cp match {
        case p: testentities.StepPlanCommandParameter.ExplicitCommandParameter =>
          p.position.some
        case _: testentities.StepPlanCommandParameter.ImplicitCommandParameter =>
          None
      },
      cp.defaultValue
    )

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
  def from(cp: testentities.StepPlanCommandParameter.CommandInput)(implicit renkuUrl: RenkuUrl): CliCommandInput =
    CliCommandInput(
      commandParameters.ResourceId(cp.asEntityId.show),
      cp.name,
      description = cp match {
        case _: testentities.StepPlanCommandParameter.CommandInput.ImplicitCommandInput =>
          None
        case p: testentities.StepPlanCommandParameter.CommandInput.MappedCommandInput =>
          p.maybeDescription
        case p: testentities.StepPlanCommandParameter.CommandInput.LocationCommandInput =>
          p.maybeDescription
      },
      cp.maybePrefix,
      position = cp match {
        case _: testentities.StepPlanCommandParameter.CommandInput.ImplicitCommandInput =>
          None
        case p: testentities.StepPlanCommandParameter.CommandInput.MappedCommandInput =>
          p.position.some
        case p: testentities.StepPlanCommandParameter.CommandInput.LocationCommandInput =>
          p.position.some
      },
      commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
      mappedTo = cp match {
        case _: testentities.StepPlanCommandParameter.CommandInput.ImplicitCommandInput =>
          None
        case p: testentities.StepPlanCommandParameter.CommandInput.MappedCommandInput =>
          from(p.mappedTo).some
        case _: testentities.StepPlanCommandParameter.CommandInput.LocationCommandInput =>
          None
      },
      cp.maybeEncodingFormat
    )

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

  def from(cp: testentities.StepPlanCommandParameter.CommandOutput)(implicit renkuUrl: RenkuUrl): CliCommandOutput =
    CliCommandOutput(
      commandParameters.ResourceId(cp.asEntityId.show),
      cp.name,
      description = cp match {
        case c: testentities.StepPlanCommandParameter.CommandOutput.MappedCommandOutput =>
          c.maybeDescription
        case c: testentities.StepPlanCommandParameter.CommandOutput.LocationCommandOutput =>
          c.maybeDescription
        case _: testentities.StepPlanCommandParameter.CommandOutput.ImplicitCommandOutput =>
          None
      },
      cp.maybePrefix,
      position = cp match {
        case c: testentities.StepPlanCommandParameter.CommandOutput.MappedCommandOutput =>
          c.position.some
        case c: testentities.StepPlanCommandParameter.CommandOutput.LocationCommandOutput =>
          c.position.some
        case _: testentities.StepPlanCommandParameter.CommandOutput.ImplicitCommandOutput =>
          None
      },
      commandParameters.ParameterDefaultValue(cp.defaultValue.value.value),
      mappedTo = cp match {
        case c: testentities.StepPlanCommandParameter.CommandOutput.MappedCommandOutput =>
          from(c.mappedTo).some
        case _: testentities.StepPlanCommandParameter.CommandOutput.LocationCommandOutput =>
          None
        case _: testentities.StepPlanCommandParameter.CommandOutput.ImplicitCommandOutput =>
          None
      },
      cp.maybeEncodingFormat,
      cp.folderCreation
    )

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

  def from(pl: testentities.ParameterLink)(implicit renkuUrl: RenkuUrl): CliParameterLink =
    CliParameterLink(
      parameterLinks.ResourceId(pl.id.asEntityId.show),
      from(pl.source),
      pl.sinks.map {
        case s: testentities.ParameterLink.Sink.Input =>
          CliParameterLink.Sink(from(s.input))
        case s: testentities.ParameterLink.Sink.Param =>
          CliParameterLink.Sink(from(s.param))
      }
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

  def from(mapping: testentities.ParameterMapping)(implicit renkuUrl: RenkuUrl): CliParameterMapping =
    CliParameterMapping(
      commandParameters.ResourceId(mapping.id.asEntityId.show),
      mapping.name,
      mapping.description,
      mapping.maybePrefix,
      position = None,
      mapping.defaultValue.map(commandParameters.ParameterDefaultValue(_)),
      mapping.mappedParam.map(fromBase)
    )

  def fromBase(param: testentities.CommandParameterBase)(implicit renkuUrl: RenkuUrl): CliParameterMapping.MappedParam =
    param match {
      case p: testentities.StepPlanCommandParameter.CommandInput     => CliParameterMapping.MappedParam(from(p))
      case p: testentities.StepPlanCommandParameter.CommandOutput    => CliParameterMapping.MappedParam(from(p))
      case p: testentities.StepPlanCommandParameter.CommandParameter => CliParameterMapping.MappedParam(from(p))
      case p: testentities.ParameterMapping                          => CliParameterMapping.MappedParam(from(p))
    }

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
