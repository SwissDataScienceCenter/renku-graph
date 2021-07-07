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

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import ch.datascience.graph.model.commandParameters._
import ch.datascience.graph.model.testentities.CommandParameterBase.CommandInput._
import ch.datascience.graph.model.testentities.CommandParameterBase.CommandOutput._
import ch.datascience.graph.model.testentities.CommandParameterBase._
import ch.datascience.graph.model.testentities._
import org.scalacheck.Gen

private object Generators {

  implicit lazy val commandParameterObjects: Gen[Position => RunPlan => CommandParameter] = for {
    name             <- commandParameterNames
    maybeDescription <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix      <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue     <- nonEmptyStrings().toGeneratorOf(ParameterDefaultValue)
  } yield (position: Position) =>
    (runPlan: RunPlan) => CommandParameter(position, name, maybeDescription, maybePrefix, defaultValue, runPlan)

  implicit lazy val locationCommandInputObjects: Gen[Position => RunPlan => LocationCommandInput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(InputDefaultValue(_))
    temporary           <- commandParameterTemporaries
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (runPlan: RunPlan) =>
      LocationCommandInput(position,
                           name,
                           maybeDescription,
                           maybePrefix,
                           defaultValue,
                           temporary,
                           maybeEncodingFormat,
                           runPlan
      )

  implicit lazy val mappedCommandInputObjects: Gen[Position => RunPlan => MappedCommandInput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(InputDefaultValue(_))
    temporary           <- commandParameterTemporaries
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (runPlan: RunPlan) =>
      MappedCommandInput(position,
                         name,
                         maybeDescription,
                         maybePrefix,
                         defaultValue,
                         temporary,
                         maybeEncodingFormat,
                         runPlan
      )

  implicit lazy val locationCommandOutputObjects: Gen[Position => RunPlan => LocationCommandOutput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(OutputDefaultValue(_))
    folderCreation      <- commandParameterFolderCreation
    temporary           <- commandParameterTemporaries
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
  } yield (position: Position) =>
    (runPlan: RunPlan) =>
      LocationCommandOutput(position,
                            name,
                            maybeDescription,
                            maybePrefix,
                            defaultValue,
                            folderCreation,
                            temporary,
                            maybeEncodingFormat,
                            runPlan
      )

  implicit lazy val mappedCommandOutputObjects: Gen[Position => RunPlan => MappedCommandOutput] = for {
    name                <- commandParameterNames
    maybeDescription    <- nonEmptyStrings().toGeneratorOf(Description).toGeneratorOfOptions
    maybePrefix         <- nonEmptyStrings().toGeneratorOf(Prefix).toGeneratorOfOptions
    defaultValue        <- entityLocations.map(OutputDefaultValue(_))
    folderCreation      <- commandParameterFolderCreation
    temporary           <- commandParameterTemporaries
    maybeEncodingFormat <- commandParameterEncodingFormats.toGeneratorOfOptions
    mappedTo            <- ioStreamOuts
  } yield (position: Position) =>
    (runPlan: RunPlan) =>
      MappedCommandOutput(position,
                          name,
                          maybeDescription,
                          maybePrefix,
                          defaultValue,
                          folderCreation,
                          temporary,
                          maybeEncodingFormat,
                          mappedTo,
                          runPlan
      )

  implicit val ioStreamOuts: Gen[IOStream.Out] = Gen.oneOf(
    IOStream.StdOut(IOStream.ResourceId((renkuBaseUrl / nonEmptyStrings().generateOne).show)),
    IOStream.StdErr(IOStream.ResourceId((renkuBaseUrl / nonEmptyStrings().generateOne).show))
  )

}
