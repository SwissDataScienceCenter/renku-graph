/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.commandParameters._
import io.renku.graph.model.testentities.StepPlanCommandParameter.CommandInput._
import io.renku.graph.model.testentities.StepPlanCommandParameter.CommandOutput._
import io.renku.graph.model.testentities.StepPlanCommandParameter._
import io.renku.graph.model.testentities.StepPlan.CommandParameters.CommandParameterFactory
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{CompositePlanGenFactory, ProjectBasedGenFactory, ProjectBasedGenFactoryOps, StepPlanGenFactory}
import io.renku.graph.model.testentities.generators.genMonad
import org.scalacheck.Gen

private object Generators {

  implicit lazy val explicitCommandParameterObjects: Gen[Position => Plan => CommandParameter] = for {
    name             <- commandParameterNames
    maybeDescription <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix      <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue     <- nonEmptyStrings().toGeneratorOf(ParameterDefaultValue)
  } yield (position: Position) =>
    (plan: Plan) => ExplicitCommandParameter(position, name, maybeDescription, maybePrefix, defaultValue, plan)

  implicit lazy val implicitCommandParameterObjects: Gen[Position => Plan => CommandParameter] = for {
    name             <- commandParameterNames
    maybeDescription <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix      <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue     <- nonEmptyStrings().toGeneratorOf(ParameterDefaultValue)
  } yield (_: Position) =>
    (plan: Plan) => ImplicitCommandParameter(name, maybeDescription, maybePrefix, defaultValue, plan)

  implicit lazy val locationCommandInputObjects: Gen[Position => Plan => LocationCommandInput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(InputDefaultValue(_))
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (plan: Plan) =>
      LocationCommandInput(position, name, maybeDescription, maybePrefix, defaultValue, maybeEncodingFormat, plan)

  implicit lazy val mappedCommandInputObjects: Gen[Position => Plan => MappedCommandInput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(InputDefaultValue(_))
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (plan: Plan) =>
      MappedCommandInput(position, name, maybeDescription, maybePrefix, defaultValue, maybeEncodingFormat, plan)

  lazy val implicitCommandInputObjects: Gen[Position => Plan => ImplicitCommandInput] = for {
    name                <- commandParameterNames
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(InputDefaultValue(_))
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (_: Position) =>
    (plan: Plan) => ImplicitCommandInput(name, maybePrefix, defaultValue, maybeEncodingFormat, plan)

  implicit lazy val locationCommandOutputObjects: Gen[Position => Plan => LocationCommandOutput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(OutputDefaultValue(_))
    folderCreation      <- commandParameterFolderCreation
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (plan: Plan) =>
      LocationCommandOutput(position,
                            name,
                            maybeDescription,
                            maybePrefix,
                            defaultValue,
                            folderCreation,
                            maybeEncodingFormat,
                            plan
      )

  implicit lazy val mappedCommandOutputObjects: Gen[Position => Plan => MappedCommandOutput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(OutputDefaultValue(_))
    folderCreation      <- commandParameterFolderCreation
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
    mappedTo            <- ioStreamOuts
  } yield (position: Position) =>
    (plan: Plan) =>
      MappedCommandOutput(position,
                          name,
                          maybeDescription,
                          maybePrefix,
                          defaultValue,
                          folderCreation,
                          maybeEncodingFormat,
                          mappedTo,
                          plan
      )

  implicit lazy val implicitCommandOutputObjects: Gen[Position => Plan => ImplicitCommandOutput] = for {
    name                <- commandParameterNames
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(OutputDefaultValue(_))
    folderCreation      <- commandParameterFolderCreation
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (_: Position) =>
    (plan: Plan) => ImplicitCommandOutput(name, maybePrefix, defaultValue, folderCreation, maybeEncodingFormat, plan)

  implicit val ioStreamOuts: Gen[IOStream.Out] = Gen.oneOf(
    IOStream.StdOut(IOStream.ResourceId((renkuUrl / nonEmptyStrings().generateOne).show)),
    IOStream.StdErr(IOStream.ResourceId((renkuUrl / nonEmptyStrings().generateOne).show))
  )

  implicit lazy val commandParametersLists: Gen[List[CommandParameterFactory]] = for {
    explicitParameters <- explicitCommandParameterObjects.toGeneratorOfList()
    locationInputs     <- locationCommandInputObjects.toGeneratorOfList()
    mappedInputs       <- mappedCommandInputObjects.toGeneratorOfList()
    implicitInputs     <- implicitCommandInputObjects.toGeneratorOfList()
    locationOutputs    <- locationCommandOutputObjects.toGeneratorOfList()
    mappedOutputs      <- mappedCommandOutputObjects.toGeneratorOfList()
    implicitOutputs    <- implicitCommandOutputObjects.toGeneratorOfList()
  } yield explicitParameters ::: locationInputs ::: mappedInputs ::: implicitInputs ::: locationOutputs ::: mappedOutputs ::: implicitOutputs

  def compositePlanGenFactory(minChildren: Int = 3, maxChildren: Int = 6): CompositePlanGenFactory =
    for {
      params <- ProjectBasedGenFactory.liftF(commandParametersLists)
      plan   <- compositePlanEntities(planEntitiesList(minChildren, maxChildren, params))
    } yield plan

  def stepPlanGenFactory: StepPlanGenFactory =
    for {
      params <- ProjectBasedGenFactory.liftF(commandParametersLists)
      plan   <- stepPlanEntities(params: _*)
    } yield plan

  def compositePlanGen(minChildren: Int = 3, maxChildren: Int = 6): Gen[CompositePlan] =
    compositePlanGenFactory(minChildren, maxChildren).generateOne
}
