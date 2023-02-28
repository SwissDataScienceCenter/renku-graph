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

import cats.syntax.all._
import io.renku.cli.model.CliMappedIOStream.StreamType
import io.renku.cli.model._
import io.renku.graph.model.testentities.HavingInvalidationTime
import io.renku.graph.model.{RenkuUrl, commandParameters, parameterLinks, plans, testentities}
import io.renku.jsonld.syntax._

trait CliPlanConverters extends CliCommonConverters {

  def from(plan: testentities.Plan)(implicit renkuUrl: RenkuUrl): CliPlan = plan.fold(
    p => CliPlan(from(p)),
    p => CliPlan(from(p)),
    p => CliPlan(from(p)),
    p => CliPlan(from(p))
  )

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
}

object CliPlanConverters extends CliPlanConverters
