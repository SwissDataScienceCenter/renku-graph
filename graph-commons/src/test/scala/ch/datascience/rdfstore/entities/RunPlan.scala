/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.implicits._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.RunPlan.{Argument, SuccessCode}
import ch.datascience.tinytypes.constraints.{NonBlank, NonNegativeInt}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}

import scala.language.postfixOps

sealed trait RunPlan {
  self: Entity =>

  val runArguments:      List[Argument]
  val runCommandInputs:  List[CommandParameter with Input]
  val runCommandOutputs: List[CommandParameter with Output]
  val runSuccessCodes:   List[SuccessCode]
}

object RunPlan {

  sealed trait ProcessRunPlan extends RunPlan {
    self: Entity =>

    val runCommand:           Command
    val maybeRunProcessOrder: Option[ProcessOrder]
  }

  sealed trait WorkflowRunPlan extends RunPlan {
    self: Entity =>

    val runSubprocesses: List[Entity with RunPlan]
  }

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def workflow(
      project:      Project,
      arguments:    List[Argument] = Nil,
      inputs:       List[Position => CommandParameter with Input] = Nil,
      outputs:      List[(Activity, Position) => CommandParameter with Output] = Nil,
      subprocesses: List[Entity with RunPlan],
      successCodes: List[SuccessCode] = Nil
  )(commitId:       CommitId, workflowFile: WorkflowFile): Entity with RunPlan =
    new Entity(commitId, workflowFile, project, maybeInvalidationActivity = None, maybeGeneration = None)
    with WorkflowRunPlan {
      override val runArguments:      List[Argument]                     = arguments
      override val runCommandInputs:  List[CommandParameter with Input]  = toParameters(inputs)
      override val runCommandOutputs: List[CommandParameter with Output] = toParameters(outputs, offset = inputs.length)
      override val runSubprocesses:   List[Entity with RunPlan]          = subprocesses
      override val runSuccessCodes:   List[SuccessCode]                  = successCodes
    }

  def process(workflowFile:      WorkflowFile,
              project:           Project,
              command:           Command,
              arguments:         List[Argument] = Nil,
              inputs:            List[Position => CommandParameter with Input] = Nil,
              outputs:           List[Activity => Position => CommandParameter with Output] = Nil,
              successCodes:      List[SuccessCode] = Nil,
              maybeProcessOrder: Option[ProcessOrder] = None)(commitId: CommitId): Entity with RunPlan =
    new Entity(commitId, workflowFile, project, maybeInvalidationActivity = None, maybeGeneration = None)
    with ProcessRunPlan {
      override val runCommand:           Command                            = command
      override val runArguments:         List[Argument]                     = arguments
      override val runCommandInputs:     List[CommandParameter with Input]  = toParameters(inputs)
      override val runCommandOutputs:    List[CommandParameter with Output] = toParameters(outputs, offset = inputs.length)
      override val runSuccessCodes:      List[SuccessCode]                  = successCodes
      override val maybeRunProcessOrder: Option[ProcessOrder]               = maybeProcessOrder
    }

  private def toParameters[T](factories: List[Position => T], offset: Int = 0): List[T] =
    factories.zipWithIndex.map {
      case (factory, idx) => factory(Position(idx + offset + 1))
    }

  private[entities] implicit def converter(implicit renkuBaseUrl: RenkuBaseUrl,
                                           fusekiBaseUrl:         FusekiBaseUrl): PartialEntityConverter[Entity with RunPlan] =
    new PartialEntityConverter[Entity with RunPlan] {
      override def convert[T <: Entity with RunPlan]: T => Either[Exception, PartialEntity] = {
        case entity: Entity with WorkflowRunPlan =>
          PartialEntity(
            EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.location),
            EntityTypes of (prov / "Plan", renku / "Run"),
            renku / "hasArguments"  -> entity.runArguments.asJsonLD,
            renku / "hasInputs"     -> entity.runCommandInputs.asJsonLD,
            renku / "hasOutputs"    -> entity.runCommandOutputs.asJsonLD,
            renku / "hasSubprocess" -> entity.runSubprocesses.asJsonLD,
            renku / "successCodes"  -> entity.runSuccessCodes.asJsonLD
          ).asRight
        case entity: Entity with ProcessRunPlan =>
          PartialEntity(
            EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.location),
            EntityTypes of (prov / "Plan", renku / "Run"),
            renku / "command"      -> entity.runCommand.asJsonLD,
            renku / "hasArguments" -> entity.runArguments.asJsonLD,
            renku / "hasInputs"    -> entity.runCommandInputs.asJsonLD,
            renku / "hasOutputs"   -> entity.runCommandOutputs.asJsonLD,
            renku / "successCodes" -> entity.runSuccessCodes.asJsonLD,
            renku / "processOrder" -> entity.maybeRunProcessOrder.asJsonLD
          ).asRight
      }
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[Entity with RunPlan] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Entity] combine entity.asPartialJsonLD[Entity with RunPlan] getOrFail
    }

  final class Argument private (val value: String) extends AnyVal with StringTinyType
  object Argument extends TinyTypeFactory[Argument](new Argument(_)) with NonBlank

  final class Command private (val value: String) extends AnyVal with StringTinyType
  object Command extends TinyTypeFactory[Command](new Command(_)) with NonBlank

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType
  implicit object SuccessCode extends TinyTypeFactory[SuccessCode](new SuccessCode(_)) with NonNegativeInt

  final class ProcessOrder private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProcessOrder extends TinyTypeFactory[ProcessOrder](new ProcessOrder(_)) with NonNegativeInt
}
