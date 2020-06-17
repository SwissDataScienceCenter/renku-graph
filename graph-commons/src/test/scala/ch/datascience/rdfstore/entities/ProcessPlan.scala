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

import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.entities.RunPlan.{Argument, Command, ProcessOrder, SuccessCode}
import ch.datascience.tinytypes.constraints.{NonBlank, NonNegativeInt}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}

final case class RunPlan(commitId:          CommitId,
                         workflowFile:      WorkflowFile,
                         project:           Project,
                         command:           Command,
                         arguments:         List[Argument],
                         commandInputs:     List[CommandParameter with Input],
                         commandOutputs:    List[CommandParameter with Output],
                         maybeSubprocess:   Option[RunPlan] = None,
                         successCodes:      List[SuccessCode] = Nil,
                         maybeProcessOrder: Option[ProcessOrder] = None)

object RunPlan {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[RunPlan] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "blob" / entity.commitId / entity.workflowFile),
        EntityTypes of (prov / "Entity", prov / "Plan", renku / "Run"),
        rdfs / "label"          -> s"${entity.workflowFile}@${entity.commitId}".asJsonLD,
        schema / "isPartOf"     -> entity.project.asJsonLD,
        prov / "atLocation"     -> entity.workflowFile.asJsonLD,
        renku / "command"       -> entity.command.asJsonLD,
        renku / "hasArguments"  -> entity.arguments.asJsonLD,
        renku / "hasInputs"     -> entity.commandInputs.asJsonLD,
        renku / "hasOutputs"    -> entity.commandOutputs.asJsonLD,
        renku / "hasSubprocess" -> entity.maybeSubprocess.asJsonLD,
        renku / "successCodes"  -> entity.successCodes.asJsonLD,
        renku / "processOrder"  -> entity.maybeProcessOrder.asJsonLD
      )
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
