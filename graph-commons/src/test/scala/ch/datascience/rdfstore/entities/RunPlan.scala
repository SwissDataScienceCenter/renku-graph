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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.config.GitLabApiUrl
import ch.datascience.rdfstore.entities.CommandParameter.Argument.ArgumentFactory
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory._
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory._
import ch.datascience.rdfstore.entities.CommandParameter.PositionInfo.Position
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.RunPlan._
import ch.datascience.tinytypes.constraints.{NonBlank, NonNegativeInt, UUID}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}

final class RunPlan private (val id:            Id,
                             val command:       Command,
                             argumentFactories: List[ArgumentFactory],
                             inputFactories:    List[PositionInputFactory],
                             outputFactories:   List[OutputFactory],
                             val subprocesses:  List[RunPlan],
                             val successCodes:  List[SuccessCode]
) {
  lazy val arguments: List[CommandParameter with Argument] = toArguments(argumentFactories)
  lazy val inputs:    List[CommandParameter with Input]    = toInputParameters(inputFactories)
  lazy val outputs:   List[CommandParameter with Output]   = toOutputParameters(outputFactories)

  def output(location: Location): Entity =
    outputs
      .collect { case output: EntityCommandParameter with Output =>
        output.entity
      }
      .find(_.location == location)
      .getOrElse(throw new IllegalStateException(s"No output entity for $location on RunPlan for Activity $id"))

  private def toArguments[T](factories: List[Position => RunPlan => T]): List[T] =
    factories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))(this)
    }

  private def toInputParameters(factories: List[InputFactory]): List[CommandParameter with Input] = {
    val offset = arguments.size

    factories.zipWithIndex.map {
      case (factory: PositionInputFactory, idx) => factory(Position(idx + offset + 1))(this)
      case (factory: MappedInputFactory, _) => factory(this)
      case (factory: NoPositionInputFactory, _) => factory(this)
    }
  }

  private def toOutputParameters(factories: List[OutputFactory]): List[CommandParameter with Output] = {
    val offset = arguments.size + inputs.size

    factories.zipWithIndex.map {
      case (factory: PositionOutputFactory, idx) => factory(Position(idx + offset + 1))(this)
      case (factory: MappedOutputFactory, _) => factory(this)
      case (factory: NoPositionOutputFactory, _) => factory(this)
    }
  }
}

object RunPlan {

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {

    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

  final class Command private (val value: String) extends AnyVal with StringTinyType
  object Command extends TinyTypeFactory[Command](new Command(_)) with NonBlank

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType
  implicit object SuccessCode extends TinyTypeFactory[SuccessCode](new SuccessCode(_)) with NonNegativeInt

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def workflow(
      argumentFactories: List[ArgumentFactory] = Nil,
      inputFactories:    List[PositionInputFactory] = Nil,
      outputFactories:   List[OutputFactory] = Nil,
      subprocesses:      List[RunPlan],
      successCodes:      List[SuccessCode] = Nil
  ): RunPlan = new RunPlan(Id.generate,
                           Command("update"),
                           argumentFactories,
                           inputFactories,
                           outputFactories,
                           subprocesses,
                           successCodes
  )

  def process(
      command:           Command,
      argumentFactories: List[ArgumentFactory] = Nil,
      inputFactories:    List[PositionInputFactory] = Nil,
      outputFactories:   List[OutputFactory] = Nil,
      successCodes:      List[SuccessCode] = Nil
  ): RunPlan = new RunPlan(Id.generate,
                           command,
                           argumentFactories,
                           inputFactories,
                           outputFactories,
                           subprocesses = Nil,
                           successCodes
  )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[RunPlan] =
    JsonLDEncoder.instance { entity =>
      implicit lazy val subprocessEncoder: JsonLDEncoder[(RunPlan, Int)] =
        JsonLDEncoder.instance[(RunPlan, Int)] { case (entity, idx) =>
          JsonLD.entity(
            id = entity.asEntityId / "subprocess" / (idx + 1),
            types = EntityTypes of renku / "OrderedSubprocess",
            renku / "index"   -> idx.asJsonLD,
            renku / "process" -> entity.asJsonLD(encoder)
          )
        }

      JsonLD.entity(
        entity.asEntityId,
        EntityTypes.of(prov / "Plan", renku / "Run"),
        renku / "command"       -> entity.command.asJsonLD,
        renku / "hasArguments"  -> entity.arguments.asJsonLD,
        renku / "hasInputs"     -> entity.inputs.asJsonLD,
        renku / "hasOutputs"    -> entity.outputs.asJsonLD,
        renku / "hasSubprocess" -> entity.subprocesses.zipWithIndex.asJsonLD,
        renku / "successCodes"  -> entity.successCodes.asJsonLD
      )
    }

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[RunPlan] =
    EntityIdEncoder.instance(entity => EntityId of (renkuBaseUrl / "plans" / entity.id))
}
