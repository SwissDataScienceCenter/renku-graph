package io.renku.cli.model.generators

import io.renku.cli.model.{CliCommandInput, CliCommandOutput, CliCommandParameter, CliMappedIOStream, CliParameterLink, CliParameterMapping}
import io.renku.generators.Generators
import io.renku.graph.model.RenkuTinyTypeGenerators
import org.scalacheck.Gen

trait CommandParameterGenerators {

  def mappedIOStreamGen: Gen[CliMappedIOStream] =
    for {
      id         <- RenkuTinyTypeGenerators.commandParameterResourceId
      streamType <- Gen.oneOf(CliMappedIOStream.StreamType.all.toList)
    } yield CliMappedIOStream(id, streamType)

  def commandParameterGen: Gen[CliCommandParameter] =
    for {
      id     <- RenkuTinyTypeGenerators.commandParameterResourceId
      name   <- RenkuTinyTypeGenerators.commandParameterNames
      descr  <- Gen.option(RenkuTinyTypeGenerators.commandParameterDescription)
      prefix <- Gen.option(RenkuTinyTypeGenerators.commandParameterPrefixGen)
      pos    <- Gen.option(RenkuTinyTypeGenerators.commandParameterPositionGen)
      defVal <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
    } yield CliCommandParameter(id, name, descr, prefix, pos, defVal)

  def commandInputGen: Gen[CliCommandInput] =
    for {
      id        <- RenkuTinyTypeGenerators.commandParameterResourceId
      name      <- RenkuTinyTypeGenerators.commandParameterNames
      descr     <- Gen.option(RenkuTinyTypeGenerators.commandParameterDescription)
      prefix    <- Gen.option(RenkuTinyTypeGenerators.commandParameterPrefixGen)
      pos       <- Gen.option(RenkuTinyTypeGenerators.commandParameterPositionGen)
      defVal    <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
      encFormat <- Gen.option(RenkuTinyTypeGenerators.commandParameterEncodingFormats)
      mapped    <- Gen.option(mappedIOStreamGen)
    } yield CliCommandInput(id, name, descr, prefix, pos, defVal, mapped, encFormat)

  def commandOutputGen: Gen[CliCommandOutput] =
    for {
      id           <- RenkuTinyTypeGenerators.commandParameterResourceId
      name         <- RenkuTinyTypeGenerators.commandParameterNames
      descr        <- Gen.option(RenkuTinyTypeGenerators.commandParameterDescription)
      prefix       <- Gen.option(RenkuTinyTypeGenerators.commandParameterPrefixGen)
      pos          <- Gen.option(RenkuTinyTypeGenerators.commandParameterPositionGen)
      defVal       <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
      encFormat    <- Gen.option(RenkuTinyTypeGenerators.commandParameterEncodingFormats)
      mapped       <- Gen.option(mappedIOStreamGen)
      createFolder <- RenkuTinyTypeGenerators.commandParameterFolderCreation
    } yield CliCommandOutput(id, name, descr, prefix, pos, defVal, mapped, encFormat, createFolder)

  def mappedParamGen: Gen[CliParameterMapping.MappedParam] =
    Gen.oneOf(
      commandParameterGen.map(CliParameterMapping.MappedParam.apply),
      commandOutputGen.map(CliParameterMapping.MappedParam.apply),
      commandInputGen.map(CliParameterMapping.MappedParam.apply)
    )

  def parameterMappingGen: Gen[CliParameterMapping] =
    for {
      id     <- RenkuTinyTypeGenerators.commandParameterResourceId
      name   <- RenkuTinyTypeGenerators.commandParameterNames
      descr  <- Gen.option(RenkuTinyTypeGenerators.commandParameterDescription)
      prefix <- Gen.option(RenkuTinyTypeGenerators.commandParameterPrefixGen)
      pos    <- Gen.option(RenkuTinyTypeGenerators.commandParameterPositionGen)
      defVal <- RenkuTinyTypeGenerators.commandParameterDefaultValueGen
      mapsTo <- mappedParamGen
    } yield CliParameterMapping(id, name, descr, prefix, pos, defVal, mapsTo)

  def parameterLinkSinkGen: Gen[CliParameterLink.Sink] =
    Gen.oneOf(
      commandParameterGen.map(CliParameterLink.Sink.apply),
      commandInputGen.map(CliParameterLink.Sink.apply)
    )

  def parameterLinkGen: Gen[CliParameterLink] =
    for {
      id     <- RenkuTinyTypeGenerators.parameterLinkResourceIdGen
      source <- commandOutputGen
      sinks  <- Generators.nonEmptyList(parameterLinkSinkGen, max = 3)
    } yield CliParameterLink(id, source, sinks)
}

object CommandParameterGenerators extends CommandParameterGenerators
