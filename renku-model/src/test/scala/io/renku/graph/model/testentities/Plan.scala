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

package io.renku.graph.model.testentities

import CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import Plan._
import cats.syntax.all._
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.Position
import io.renku.graph.model.entityModel.Location
import io.renku.graph.model.plans._
import io.renku.tinytypes._
import io.renku.tinytypes.constraints._

case class Plan(id:                        Id,
                name:                      Name,
                maybeDescription:          Option[Description],
                command:                   Command,
                maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                keywords:                  List[Keyword],
                commandParameterFactories: List[Plan => CommandParameterBase],
                successCodes:              List[SuccessCode]
) {
  private val commandParameters: List[CommandParameterBase] = commandParameterFactories.map(_.apply(this))
  val parameters:                List[CommandParameter]     = commandParameters.collect { case param: CommandParameter => param }
  val inputs:                    List[CommandInput]         = commandParameters.collect { case in: CommandInput => in }
  val outputs:                   List[CommandOutput]        = commandParameters.collect { case out: CommandOutput => out }

  def getInput(location: Location): Option[CommandInput] = inputs.find(_.defaultValue.value == location)
}

object Plan {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def apply(
      name:                      Name,
      command:                   Command,
      commandParameterFactories: List[Position => Plan => CommandParameterBase]
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
    successCodes = Nil
  )

  object CommandParameters {

    type CommandParameterFactory = Position => Plan => CommandParameterBase

    def of(parameters: CommandParameterFactory*): List[CommandParameterFactory] = parameters.toList
  }

  implicit def toEntitiesPlan(implicit renkuBaseUrl: RenkuBaseUrl): Plan => entities.Plan =
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
        maybeInvalidationTime
      )
    }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Plan] =
    JsonLDEncoder.instance(_.to[entities.Plan].asJsonLD)

  implicit def entityIdEncoder[R <: Plan](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => EntityId of renkuBaseUrl / "plans" / plan.id)

  final class Id private (val value: String) extends AnyVal with StringTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with UUID {
    def generate: Id = Id {
      java.util.UUID.randomUUID.toString
    }
  }
}
