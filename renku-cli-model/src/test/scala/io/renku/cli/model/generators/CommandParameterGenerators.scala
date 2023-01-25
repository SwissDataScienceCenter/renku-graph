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

package io.renku.cli.model.generators

import io.renku.cli.model._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.commandParameters.IOStream
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

trait CommandParameterGenerators {

  def mappedIOStreamGen(implicit renkuUrl: RenkuUrl): Gen[CliMappedIOStream] = for {
    streamType <- Gen.oneOf(CliMappedIOStream.StreamType.all.toList)
  } yield CliMappedIOStream(IOStream.ResourceId((renkuUrl / "iostreams" / streamType.name).value), streamType)

  def commandParameterGen: Gen[CliCommandParameter] = for {
    id     <- RenkuTinyTypeGenerators.commandParameterResourceId
    name   <- RenkuTinyTypeGenerators.commandParameterNames
    descr  <- RenkuTinyTypeGenerators.commandParameterDescription.toGeneratorOfOptions
    prefix <- RenkuTinyTypeGenerators.commandParameterPrefixGen.toGeneratorOfOptions
    pos    <- RenkuTinyTypeGenerators.commandParameterPositionGen.toGeneratorOfOptions
    defVal <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
  } yield CliCommandParameter(id, name, descr, prefix, pos, defVal)

  def commandInputGen(implicit renkuUrl: RenkuUrl): Gen[CliCommandInput] = for {
    id        <- RenkuTinyTypeGenerators.commandParameterResourceId
    name      <- RenkuTinyTypeGenerators.commandParameterNames
    descr     <- RenkuTinyTypeGenerators.commandParameterDescription.toGeneratorOfOptions
    prefix    <- RenkuTinyTypeGenerators.commandParameterPrefixGen.toGeneratorOfOptions
    pos       <- RenkuTinyTypeGenerators.commandParameterPositionGen.toGeneratorOfOptions
    defVal    <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
    encFormat <- RenkuTinyTypeGenerators.commandParameterEncodingFormats.toGeneratorOfOptions
    mapped    <- mappedIOStreamGen.toGeneratorOfOptions
  } yield CliCommandInput(id, name, descr, prefix, pos, defVal, mapped, encFormat)

  def commandOutputGen(implicit renkuUrl: RenkuUrl): Gen[CliCommandOutput] = for {
    id           <- RenkuTinyTypeGenerators.commandParameterResourceId
    name         <- RenkuTinyTypeGenerators.commandParameterNames
    descr        <- RenkuTinyTypeGenerators.commandParameterDescription.toGeneratorOfOptions
    prefix       <- RenkuTinyTypeGenerators.commandParameterPrefixGen.toGeneratorOfOptions
    pos          <- RenkuTinyTypeGenerators.commandParameterPositionGen.toGeneratorOfOptions
    defVal       <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
    encFormat    <- RenkuTinyTypeGenerators.commandParameterEncodingFormats.toGeneratorOfOptions
    mapped       <- mappedIOStreamGen.toGeneratorOfOptions
    createFolder <- RenkuTinyTypeGenerators.commandParameterFolderCreation
  } yield CliCommandOutput(id, name, descr, prefix, pos, defVal, mapped, encFormat, createFolder)

  def mappedParamGen(implicit renkuUrl: RenkuUrl): Gen[CliParameterMapping.MappedParam] =
    Gen.oneOf(
      commandParameterGen.map(CliParameterMapping.MappedParam.apply),
      commandOutputGen.map(CliParameterMapping.MappedParam.apply),
      commandInputGen.map(CliParameterMapping.MappedParam.apply)
    )

  def parameterMappingGen(implicit renkuUrl: RenkuUrl): Gen[CliParameterMapping] = for {
    id     <- RenkuTinyTypeGenerators.commandParameterResourceId
    name   <- RenkuTinyTypeGenerators.commandParameterNames
    descr  <- RenkuTinyTypeGenerators.commandParameterDescription.toGeneratorOfOptions
    prefix <- RenkuTinyTypeGenerators.commandParameterPrefixGen.toGeneratorOfOptions
    pos    <- RenkuTinyTypeGenerators.commandParameterPositionGen.toGeneratorOfOptions
    defVal <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen.toGeneratorOfOptions
    mapsTo <- mappedParamGen.toGeneratorOfNonEmptyList(max = 1)
  } yield CliParameterMapping(id, name, descr, prefix, pos, defVal, mapsTo)

  def parameterLinkSinkGen(implicit renkuUrl: RenkuUrl): Gen[CliParameterLink.Sink] =
    Gen.oneOf(
      commandParameterGen.map(CliParameterLink.Sink.apply),
      commandInputGen.map(CliParameterLink.Sink.apply)
    )

  def parameterLinkGen(implicit renkuUrl: RenkuUrl): Gen[CliParameterLink] = for {
    id     <- RenkuTinyTypeGenerators.parameterLinkResourceIdGen
    source <- commandOutputGen
    sinks  <- parameterLinkSinkGen.toGeneratorOfNonEmptyList(max = 3)
  } yield CliParameterLink(id, source, sinks)
}

object CommandParameterGenerators extends CommandParameterGenerators
