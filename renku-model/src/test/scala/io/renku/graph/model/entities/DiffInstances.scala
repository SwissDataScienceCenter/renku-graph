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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import com.softwaremill.diffx._
import io.renku.graph.model.commandParameters
import io.renku.tinytypes.{BooleanTinyType, InstantTinyType, IntTinyType, StringTinyType}

trait DiffInstances {

  implicit def stringTinyTypeDiff[A <: StringTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value)

  implicit def instantTinyTypeDiff[A <: InstantTinyType]: Diff[A] =
    Diff.diffForString.contramap(_.value.toString)

  implicit def intTinyTypeDiff[A <: IntTinyType]: Diff[A] =
    Diff.diffForNumeric[Int].contramap(_.value)

  implicit def boolTinyTypeDiff[A <: BooleanTinyType]: Diff[A] =
    Diff.diffForBoolean.contramap(_.value)

  implicit def nonEmptyListDiff[A: Diff]: Diff[NonEmptyList[A]] =
    Diff.diffForSeq[List, A].contramap(_.toList)

  implicit val personWithGitlabIdDiff: Diff[Person.WithGitLabId] = Diff.derived[Person.WithGitLabId]
  implicit val personWithEmailDiff:    Diff[Person.WithEmail]    = Diff.derived[Person.WithEmail]
  implicit val personWithNameDiff:     Diff[Person.WithNameOnly] = Diff.derived[Person.WithNameOnly]

  implicit val personDiff: Diff[Person] = Diff.derived[Person]

  implicit val explCommandParamDiff: Diff[StepPlanCommandParameter.ExplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ExplicitCommandParameter]

  implicit val implCommandParamDiff: Diff[StepPlanCommandParameter.ImplicitCommandParameter] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandParameter]

  implicit val commandInputDiff: Diff[StepPlanCommandParameter.CommandInput] =
    Diff.derived[StepPlanCommandParameter.CommandInput]
  implicit val commandOutputDiff: Diff[StepPlanCommandParameter.CommandOutput] =
    Diff.derived[StepPlanCommandParameter.CommandOutput]

  implicit lazy val locationCommandOutputDiff: Diff[StepPlanCommandParameter.LocationCommandOutput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandOutput]

  implicit lazy val implicitCommandOutputDiff: Diff[StepPlanCommandParameter.ImplicitCommandOutput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandOutput]

  implicit lazy val mappedCommandOutputDiff: Diff[StepPlanCommandParameter.MappedCommandOutput] =
    Diff.derived[StepPlanCommandParameter.MappedCommandOutput]

  implicit lazy val outputDefaultValueDiff: Diff[commandParameters.OutputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val inputDefaultValueDiff: Diff[commandParameters.InputDefaultValue] =
    Diff.diffForString.contramap(_.value.value)

  implicit val ioStreamInDiff: Diff[commandParameters.IOStream.In] = Diff.derived[commandParameters.IOStream.In]

  implicit val ioStreamOutDiff: Diff[commandParameters.IOStream.Out] = Diff.derived[commandParameters.IOStream.Out]

  implicit val ioStreamStdInDiff: Diff[commandParameters.IOStream.StdIn] =
    Diff.derived[commandParameters.IOStream.StdIn]
  implicit val ioStreamStdOutDiff: Diff[commandParameters.IOStream.StdOut] =
    Diff.derived[commandParameters.IOStream.StdOut]
  implicit val ioStreamErrOutDiff: Diff[commandParameters.IOStream.StdErr] =
    Diff.derived[commandParameters.IOStream.StdErr]

  implicit val implicitCommandInputDiff: Diff[StepPlanCommandParameter.ImplicitCommandInput] =
    Diff.derived[StepPlanCommandParameter.ImplicitCommandInput]

  implicit val locationCommandInputDiff: Diff[StepPlanCommandParameter.LocationCommandInput] =
    Diff.derived[StepPlanCommandParameter.LocationCommandInput]

  implicit val commandParamDiff: Diff[StepPlanCommandParameter.CommandParameter] =
    Diff.derived[StepPlanCommandParameter.CommandParameter]

  implicit val planDerivationDiff: Diff[Plan.Derivation] =
    Diff.derived[Plan.Derivation]

  implicit val parameterMappingDiff: Diff[ParameterMapping] =
    Diff.derived[ParameterMapping]

  implicit val parameterLinkDiff: Diff[ParameterLink] =
    Diff.derived[ParameterLink]

  implicit val stepPlanDiff: Diff[StepPlan] = Diff.derived[StepPlan]

  implicit val compositePlanDiff: Diff[CompositePlan] = Diff.derived[CompositePlan]

  implicit val planDiff: Diff[Plan] = Diff.derived[Plan]

}

object DiffInstances extends DiffInstances
