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

package ch.datascience.graph.model.testentities

import CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import Plan._
import cats.syntax.all._
import ch.datascience.graph.model.commandParameters.Position
import ch.datascience.graph.model.entityModel.Location
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.plans._
import ch.datascience.graph.model._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

case class Plan(id:                        Id,
                name:                      Name,
                maybeDescription:          Option[Description],
                command:                   Command,
                maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                keywords:                  List[Keyword],
                commandParameterFactories: List[Plan => CommandParameterBase],
                successCodes:              List[SuccessCode],
                project:                   Project[ForksCount]
) {
  private lazy val commandParameters: List[CommandParameterBase] = commandParameterFactories.map(_.apply(this))
  lazy val parameters:                List[CommandParameter]     = commandParameters.collect { case param: CommandParameter => param }
  lazy val inputs:                    List[CommandInput]         = commandParameters.collect { case in: CommandInput => in }
  lazy val outputs:                   List[CommandOutput]        = commandParameters.collect { case out: CommandOutput => out }

  def getInput(location: Location): Option[CommandInput] = inputs.find(_.defaultValue.value == location)
}

object Plan {

  import io.renku.jsonld._
  import JsonLDEncoder._
  import io.renku.jsonld.syntax._

  def apply(
      name:                      Name,
      command:                   Command,
      commandParameterFactories: List[Position => Plan => CommandParameterBase],
      project:                   Project[ForksCount]
  ): Plan = Plan(
    Id.generate,
    name,
    maybeDescription = None,
    command,
    maybeProgrammingLanguage = None,
    keywords = Nil,
    commandParameterFactories = commandParameterFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes = Nil,
    project
  )

  object CommandParameters {

    type CommandParameterFactory = Position => Plan => CommandParameterBase

    def of(parameters: CommandParameterFactory*): List[CommandParameterFactory] = parameters.toList
  }

  implicit lazy val toEntitiesPlan: Plan => entities.Plan =
    plan => {
      val maybeInvalidationTime = plan match {
        case plan: Plan with HavingInvalidationTime => plan.invalidationTime.some
        case _ => None
      }

      entities.Plan(
        plans.ResourceId(plan.asEntityId.show),
        plan.name,
        plan.maybeDescription,
        plan.command,
        plan.maybeProgrammingLanguage,
        plan.keywords,
        plan.parameters.map(_.to[entities.CommandParameterBase.CommandParameter]),
        plan.inputs.map(_.to[entities.CommandParameterBase.CommandInput]),
        plan.outputs.map(_.to[entities.CommandParameterBase.CommandOutput]),
        plan.successCodes,
        projects.ResourceId(plan.project.asEntityId),
        maybeInvalidationTime
      )
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Plan] = JsonLDEncoder.instance {
    case plan: Plan with HavingInvalidationTime =>
      JsonLD.entity(
        plan.asEntityId,
        EntityTypes.of(prov / "Plan", renku / "Plan"),
        schema / "name"                -> plan.name.asJsonLD,
        schema / "description"         -> plan.maybeDescription.asJsonLD,
        renku / "command"              -> plan.command.asJsonLD,
        schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
        schema / "keywords"            -> plan.keywords.asJsonLD,
        renku / "hasArguments"         -> plan.parameters.asJsonLD,
        renku / "hasInputs"            -> plan.inputs.asJsonLD,
        renku / "hasOutputs"           -> plan.outputs.asJsonLD,
        renku / "successCodes"         -> plan.successCodes.asJsonLD,
        schema / "isPartOf"            -> plan.project.asEntityId.asJsonLD,
        prov / "invalidatedAtTime"     -> plan.invalidationTime.asJsonLD
      )
    case plan: Plan =>
      JsonLD.entity(
        plan.asEntityId,
        EntityTypes.of(prov / "Plan", renku / "Plan"),
        schema / "name"                -> plan.name.asJsonLD,
        schema / "description"         -> plan.maybeDescription.asJsonLD,
        renku / "command"              -> plan.command.asJsonLD,
        schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
        schema / "keywords"            -> plan.keywords.asJsonLD,
        renku / "hasArguments"         -> plan.parameters.asJsonLD,
        renku / "hasInputs"            -> plan.inputs.asJsonLD,
        renku / "hasOutputs"           -> plan.outputs.asJsonLD,
        renku / "successCodes"         -> plan.successCodes.asJsonLD,
        schema / "isPartOf"            -> plan.project.asEntityId.asJsonLD
      )
  }

  implicit def entityIdEncoder[R <: Plan](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => EntityId of renkuBaseUrl / "plans" / plan.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {

    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }

}
