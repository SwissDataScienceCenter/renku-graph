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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.entities.StepPlanCommandParameter
import io.renku.graph.model.{commandParameters, entities, plans}

private trait CliConversionFunctions {

  def findStepPlan(id: plans.ResourceId, allPlans: List[entities.Plan]): Option[entities.StepPlan] =
    allPlans.find(_.resourceId == id) >>= {
      case p: entities.StepPlan => p.some
      case _ => None
    }

  def findStepPlanOrFail(id: plans.ResourceId, allPlans: List[entities.Plan]): entities.StepPlan =
    findStepPlan(id, allPlans).getOrElse(fail(show"No StepPlan with $id"))

  def collectAllPlans(ids: NonEmptyList[plans.ResourceId], allPlans: List[entities.Plan]): NonEmptyList[entities.Plan] =
    allPlans.filter(p => ids.contains_(p.resourceId)) match {
      case found if found.size == ids.size => NonEmptyList.fromListUnsafe(found)
      case _                               => fail("Not all plans found")
    }

  def findCommandOutput(paramId:  commandParameters.ResourceId,
                        allPlans: List[entities.Plan]
  ): entities.StepPlanCommandParameter.CommandOutput =
    allPlans
      .find(commandOutputPF(paramId)(_).isDefined)
      .flatMap(commandOutputPF(paramId))
      .getOrElse(fail(show"No OutputParameter with $paramId"))

  def collectAllParameters(ids:      NonEmptyList[commandParameters.ResourceId],
                           allPlans: List[entities.Plan]
  ): List[entities.StepPlanCommandParameter.CommandParameter] =
    allPlans.flatMap(commandParametersPF).filter(p => ids.contains_(p.resourceId))

  def collectAllInputParameters(ids:      NonEmptyList[commandParameters.ResourceId],
                                allPlans: List[entities.Plan]
  ): List[entities.StepPlanCommandParameter.CommandInput] =
    allPlans.flatMap(commandInputsPF).filter(p => ids.contains_(p.resourceId))

  def collectAllOutputParameters(ids:      NonEmptyList[commandParameters.ResourceId],
                                 allPlans: List[entities.Plan]
  ): List[entities.StepPlanCommandParameter.CommandOutput] =
    allPlans.flatMap(commandOutputsPF).filter(p => ids.contains_(p.resourceId))

  def collectAllParameterMappings(ids:      NonEmptyList[commandParameters.ResourceId],
                                  allPlans: List[entities.Plan]
  ): List[entities.ParameterMapping] =
    allPlans.flatMap(commandMappingsPF).filter(p => ids.contains_(p.resourceId))

  private lazy val commandParametersPF
      : PartialFunction[entities.Plan, List[entities.StepPlanCommandParameter.CommandParameter]] =
    _.fold[List[StepPlanCommandParameter.CommandParameter]](_.parameters, _.parameters, _ => Nil, _ => Nil)

  private lazy val commandInputsPF
      : PartialFunction[entities.Plan, List[entities.StepPlanCommandParameter.CommandInput]] =
    _.fold[List[StepPlanCommandParameter.CommandInput]](_.inputs, _.inputs, _ => Nil, _ => Nil)

  private lazy val commandOutputsPF
      : PartialFunction[entities.Plan, List[entities.StepPlanCommandParameter.CommandOutput]] =
    _.fold[List[StepPlanCommandParameter.CommandOutput]](_.outputs, _.outputs, _ => Nil, _ => Nil)

  private lazy val commandMappingsPF: PartialFunction[entities.Plan, List[entities.ParameterMapping]] =
    _.fold[List[entities.ParameterMapping]](_ => Nil, _ => Nil, _.mappings, _.mappings)

  private def commandOutputPF(
      paramId: commandParameters.ResourceId
  ): PartialFunction[entities.Plan, Option[entities.StepPlanCommandParameter.CommandOutput]] =
    _.fold[Option[StepPlanCommandParameter.CommandOutput]](_.outputs.find(_.resourceId == paramId),
                                                           _.outputs.find(_.resourceId == paramId),
                                                           _ => None,
                                                           _ => None
    )

  private def fail(message: String) = throw new Exception(message)
}

private object CliConversionFunctions extends CliConversionFunctions
