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

package io.renku.graph.model.testentities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.Position
import io.renku.graph.model.entityModel.Location
import io.renku.graph.model.plans._
import io.renku.graph.model.testentities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}

sealed trait Plan {
  val id:               Identifier
  val name:             Name
  val maybeDescription: Option[Description]
  val creators:         Set[Person]
  val dateCreated:      DateCreated
  val keywords:         List[Keyword]
}

object Plan {

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def of(name:                      Name,
         maybeCommand:              Option[Command],
         dateCreated:               DateCreated,
         commandParameterFactories: List[Position => Plan => CommandParameterBase]
  ): StepPlan = StepPlan.of(name, maybeCommand, dateCreated, commandParameterFactories)

  implicit class PlanOps[P <: Plan](plan: P) {

    def to[T](implicit convert: P => T): T = convert(plan)

    def invalidate(time: InvalidationTime): ValidatedNel[String, P with HavingInvalidationTime] = plan match {
      case p: StepPlan => p.invalidate(time).asInstanceOf
    }

    def replaceCreators(creators: Set[Person]): P = plan match {
      case p: StepPlan => p.copy(creators = creators).asInstanceOf
    }

    def replacePlanName(to: plans.Name): P = plan match {
      case p: StepPlan => p.copy(name = to).asInstanceOf
    }

    def replacePlanKeywords(to: List[plans.Keyword]): P = plan match {
      case p: StepPlan => p.copy(keywords = to).asInstanceOf
    }

    def replacePlanDesc(to: Option[plans.Description]): P = plan match {
      case p: StepPlan => p.copy(maybeDescription = to).asInstanceOf
    }

    def replacePlanDateCreated(to: plans.DateCreated): P = plan match {
      case p: StepPlan => p.copy(dateCreated = to).asInstanceOf
    }
  }

  implicit def toEntitiesPlan[P <: Plan](implicit renkuUrl: RenkuUrl): P => entities.Plan = { case p: StepPlan =>
    p.to[entities.Plan](StepPlan.toEntitiesStepPlan)
  }

  implicit def encoder[P <: Plan](implicit renkuUrl: RenkuUrl, graphClass: GraphClass): JsonLDEncoder[P] =
    JsonLDEncoder.instance(_.to[entities.Plan].asJsonLD)

  implicit def entityIdEncoder[R <: Plan](implicit renkuUrl: RenkuUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => ResourceId(plan.id).asEntityId)
}

case class StepPlan(id:                        Identifier,
                    name:                      Name,
                    maybeDescription:          Option[Description],
                    creators:                  Set[Person],
                    dateCreated:               DateCreated,
                    keywords:                  List[Keyword],
                    maybeCommand:              Option[Command],
                    maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                    successCodes:              List[SuccessCode],
                    commandParameterFactories: List[StepPlan => CommandParameterBase]
) extends Plan
    with StepPlanAlg {

  private lazy val commandParameters: List[CommandParameterBase] = commandParameterFactories.map(_.apply(this))
  lazy val parameters: List[CommandParameter] = commandParameters.collect { case param: CommandParameter => param }
  lazy val inputs:     List[CommandInput]     = commandParameters.collect { case in: CommandInput => in }
  lazy val outputs:    List[CommandOutput]    = commandParameters.collect { case out: CommandOutput => out }

  def getInput(location: Location): Option[CommandInput] = inputs.find(_.defaultValue.value == location)
}

trait StepPlanAlg {
  this: StepPlan =>

  def invalidate(time: InvalidationTime): ValidatedNel[String, StepPlan with HavingInvalidationTime] =
    Validated.condNel(
      test = (time.value compareTo dateCreated.value) >= 0,
      new StepPlan(
        planIdentifiers.generateOne,
        name,
        maybeDescription,
        creators,
        dateCreated,
        keywords,
        maybeCommand,
        maybeProgrammingLanguage,
        successCodes,
        commandParameterFactories
      ) with HavingInvalidationTime {
        override val invalidationTime: InvalidationTime = time
      },
      s"Invalidation time $time on StepPlan with id: $id is older than dateCreated"
    )
}

object StepPlan {

  import io.renku.generators.Generators.Implicits._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def of(name:                      Name,
         maybeCommand:              Option[Command],
         dateCreated:               DateCreated,
         commandParameterFactories: List[Position => Plan => CommandParameterBase]
  ): StepPlan = StepPlan(
    planIdentifiers.generateOne,
    name,
    maybeDescription = None,
    creators = Set.empty,
    dateCreated,
    keywords = Nil,
    maybeCommand = maybeCommand,
    maybeProgrammingLanguage = None,
    commandParameterFactories = commandParameterFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes = Nil
  )

  object CommandParameters {

    type CommandParameterFactory = Position => Plan => CommandParameterBase

    def of(parameters: CommandParameterFactory*): List[CommandParameterFactory] = parameters.toList
  }

  implicit def toEntitiesStepPlan(implicit renkuUrl: RenkuUrl): StepPlan => entities.Plan = plan => {
    val maybeInvalidationTime = plan match {
      case plan: StepPlan with HavingInvalidationTime => plan.invalidationTime.some
      case _ => None
    }

    entities.Plan(
      plans.ResourceId(plan.asEntityId.show),
      plan.name,
      plan.maybeDescription,
      plan.maybeCommand,
      plan.creators.map(_.to[entities.Person]),
      plan.dateCreated,
      plan.maybeProgrammingLanguage,
      plan.keywords,
      plan.parameters.map(_.to[entities.CommandParameterBase.CommandParameter]),
      plan.inputs.map(_.to[entities.CommandParameterBase.CommandInput]),
      plan.outputs.map(_.to[entities.CommandParameterBase.CommandOutput]),
      plan.successCodes,
      maybeInvalidationTime
    )
  }

  implicit def encoder(implicit renkuUrl: RenkuUrl, graphClass: GraphClass): JsonLDEncoder[StepPlan] =
    JsonLDEncoder.instance(_.to[entities.Plan].asJsonLD)

  implicit def entityIdEncoder[R <: Plan](implicit renkuUrl: RenkuUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => ResourceId(plan.id).asEntityId)
}
