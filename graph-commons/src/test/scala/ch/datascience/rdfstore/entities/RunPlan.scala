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
import ch.datascience.rdfstore.entities.CommandParameter.Argument.ArgumentFactory
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory
import ch.datascience.rdfstore.entities.CommandParameter.Input.InputFactory.{ActivityPositionInputFactory, PositionInputFactory}
import ch.datascience.rdfstore.entities.CommandParameter.Output.OutputFactory
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.tinytypes.constraints.{NonBlank, NonNegativeInt}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}

import scala.language.postfixOps

sealed trait RunPlan {
  self: Entity =>

  import RunPlan._

  val identifier: Identifier = Identifier.generate
  val runArguments:      List[CommandParameter with Argument]
  val runCommandInputs:  List[CommandParameter with Input]
  val runCommandOutputs: List[CommandParameter with Output]
  val runSuccessCodes:   List[SuccessCode]

  def output(location: Location): Entity with Artifact =
    runCommandOutputs
      .flatMap {
        case output: EntityCommandParameter with Output => Some(output.entity)
        case _ => None
      }
      .find(_.location == location)
      .getOrElse(throw new IllegalStateException(s"No output entity for $location on RunPlan for Activity $commitId"))
}

object RunPlan {

  final class Identifier private (val value: String) extends AnyVal with StringTinyType
  implicit object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with NonBlank {
    import java.util.UUID.randomUUID
    def generate: Identifier = Identifier(randomUUID.toString)
  }

  sealed trait ProcessRunPlan extends RunPlan {
    self: Entity =>

    val runCommand:           Command
    val maybeRunProcessOrder: Option[ProcessOrder]
  }

  sealed trait WorkflowRunPlan extends RunPlan {
    self: Entity =>

    val runSubprocesses: List[Entity with ProcessRunPlan]
  }

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def workflow(
      arguments:    List[ArgumentFactory] = Nil,
      inputs:       List[PositionInputFactory] = Nil,
      outputs:      List[OutputFactory] = Nil,
      subprocesses: List[Entity with ProcessRunPlan],
      successCodes: List[SuccessCode] = Nil
  )(project:        Project)(activity: Activity)(workflowFile: WorkflowFile): Entity with WorkflowRunPlan =
    new Entity(activity.commitId, workflowFile, project, maybeInvalidationActivity = None, maybeGeneration = None)
    with WorkflowRunPlan {
      override val runArguments: List[Argument] = toParameters(arguments, this)
      override val runCommandInputs: List[CommandParameter with Input] =
        toParameters(inputs, offset = arguments.length, this)
      override val runCommandOutputs: List[CommandParameter with Output] =
        toOutputParameters(outputs, offset = arguments.length + inputs.length, this)(activity)
      override val runSubprocesses: List[Entity with ProcessRunPlan] = subprocesses
      override val runSuccessCodes: List[SuccessCode]                = successCodes
    }

  def process(
      workflowFile: WorkflowFile,
      command:      Command,
      arguments:    List[ArgumentFactory] = Nil,
      inputs:       List[InputFactory] = Nil,
      outputs:      List[OutputFactory] = Nil,
      successCodes: List[SuccessCode] = Nil
  )(activity:       Activity): Entity with ProcessRunPlan =
    new Entity(activity.commitId,
               workflowFile,
               activity.project,
               maybeInvalidationActivity = None,
               maybeGeneration           = None) with ProcessRunPlan {
      override val runCommand:   Command        = command
      override val runArguments: List[Argument] = toParameters(arguments, this)
      override val runCommandInputs: List[CommandParameter with Input] =
        toInputParameters(inputs, offset = arguments.length, this)(activity)
      override val runCommandOutputs: List[CommandParameter with Output] =
        toOutputParameters(outputs, offset = arguments.length + inputs.length, this)(activity)
      override val runSuccessCodes:      List[SuccessCode]    = successCodes
      override val maybeRunProcessOrder: Option[ProcessOrder] = None
    }

  private def toParameters[T](factories: List[Position => Entity with RunPlan => T],
                              runPlan:   Entity with RunPlan): List[T] =
    factories.zipWithIndex.map {
      case (factory, idx) => factory(Position(idx + 1))(runPlan)
    }

  private def toParameters[T](factories: List[Position => Entity with RunPlan => T],
                              offset:    Int,
                              runPlan:   Entity with RunPlan): List[T] =
    factories.zipWithIndex.map {
      case (factory, idx) => factory(Position(idx + offset + 1))(runPlan)
    }

  private def toOutputParameters(
      factories: List[OutputFactory],
      offset:    Int,
      runPlan:   Entity with RunPlan
  )(activity:    Activity): List[CommandParameter with Output] =
    factories.zipWithIndex.map {
      case (factory, idx) => factory(activity)(Position(idx + offset + 1))(runPlan)
    }

  private def toInputParameters(
      factories: List[InputFactory],
      offset:    Int,
      runPlan:   Entity with RunPlan
  )(activity:    Activity): List[CommandParameter with Input] =
    factories.zipWithIndex.map {
      case (factory: ActivityPositionInputFactory, idx) =>
        factory(activity)(Position(idx + offset + 1))(runPlan)
      case (factory: PositionInputFactory, idx) => factory(Position(idx + offset + 1))(runPlan)
    }

  private[entities] implicit def workflowRunPlanConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Entity with WorkflowRunPlan] =
    new PartialEntityConverter[Entity with WorkflowRunPlan] {
      override def convert[T <: Entity with WorkflowRunPlan]: T => Either[Exception, PartialEntity] = { entity =>
        PartialEntity(
          EntityTypes of (prov / "Plan", renku / "Run"),
          renku / "hasArguments"  -> entity.runArguments.asJsonLD,
          renku / "hasInputs"     -> entity.runCommandInputs.asJsonLD,
          renku / "hasOutputs"    -> entity.runCommandOutputs.asJsonLD,
          renku / "hasSubprocess" -> entity.runSubprocesses.asJsonLD,
          renku / "successCodes"  -> entity.runSuccessCodes.asJsonLD
        ).asRight
      }

      override def toEntityId: Entity with WorkflowRunPlan => Option[EntityId] =
        entity => (EntityId of (renkuBaseUrl / "runs" / entity.identifier)).some
    }

  private[entities] implicit def processRunPlanConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Entity with ProcessRunPlan] =
    new PartialEntityConverter[Entity with ProcessRunPlan] {
      override def convert[T <: Entity with ProcessRunPlan]: T => Either[Exception, PartialEntity] = { entity =>
        PartialEntity(
          EntityTypes of (prov / "Plan", renku / "Run"),
          renku / "command"      -> entity.runCommand.asJsonLD,
          renku / "hasArguments" -> entity.runArguments.asJsonLD,
          renku / "hasInputs"    -> entity.runCommandInputs.asJsonLD,
          renku / "hasOutputs"   -> entity.runCommandOutputs.asJsonLD,
          renku / "successCodes" -> entity.runSuccessCodes.asJsonLD,
          renku / "processOrder" -> entity.maybeRunProcessOrder.asJsonLD
        ).asRight
      }

      override def toEntityId: Entity with ProcessRunPlan => Option[EntityId] =
        entity => (EntityId of (renkuBaseUrl / "runs" / entity.identifier)).some
    }

  private[entities] implicit def runPlanConverter(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): PartialEntityConverter[Entity with RunPlan] = new PartialEntityConverter[Entity with RunPlan] {
    override def convert[T <: Entity with RunPlan]: T => Either[Exception, PartialEntity] = {
      case rp: Entity with WorkflowRunPlan =>
        implicitly[PartialEntityConverter[Entity with WorkflowRunPlan]].convert(rp)
      case rp: Entity with ProcessRunPlan =>
        implicitly[PartialEntityConverter[Entity with ProcessRunPlan]].convert(rp)
    }

    override def toEntityId: Entity with RunPlan => Option[EntityId] = {
      case rp: Entity with WorkflowRunPlan =>
        implicitly[PartialEntityConverter[Entity with WorkflowRunPlan]].toEntityId(rp)
      case rp: Entity with ProcessRunPlan =>
        implicitly[PartialEntityConverter[Entity with ProcessRunPlan]].toEntityId(rp)
    }
  }

  implicit def workflowRUnPlanEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): JsonLDEncoder[Entity with WorkflowRunPlan] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Entity] combine entity.asPartialJsonLD[Entity with WorkflowRunPlan] getOrFail
    }

  implicit def processRunPlanEncoder(
      implicit renkuBaseUrl: RenkuBaseUrl,
      fusekiBaseUrl:         FusekiBaseUrl
  ): JsonLDEncoder[Entity with ProcessRunPlan] =
    JsonLDEncoder.instance { entity =>
      entity.asPartialJsonLD[Entity] combine entity.asPartialJsonLD[Entity with ProcessRunPlan] getOrFail
    }

  implicit class RunPlanOps(runPlan: RunPlan) {
    def asUsages(step: Step): List[Usage] =
      runPlan.runCommandInputs.foldLeft(List.empty[Usage]) {
        case (usages, input: EntityCommandParameter with Input) => usages :+ Usage.factory(input)(step)
        case (usages, _) => usages
      }

    def asUsages: List[Usage] =
      runPlan.runCommandInputs.foldLeft(List.empty[Usage]) {
        case (usages, input: EntityCommandParameter with Input) => usages :+ Usage(input)
        case (usages, _) => usages
      }
  }

  final class Command private (val value: String) extends AnyVal with StringTinyType
  object Command extends TinyTypeFactory[Command](new Command(_)) with NonBlank

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType
  implicit object SuccessCode extends TinyTypeFactory[SuccessCode](new SuccessCode(_)) with NonNegativeInt

  final class ProcessOrder private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProcessOrder extends TinyTypeFactory[ProcessOrder](new ProcessOrder(_)) with NonNegativeInt
}
