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

package io.renku.graph.model
package testentities

import CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import commandParameters.Position
import entityModel.Location
import plans.{Command, DateCreated, DerivedFrom, Description, Identifier, Keyword, Name, ProgrammingLanguage, ResourceId, SuccessCode}

trait StepPlan extends Plan {
  val maybeCommand:              Option[Command]
  val maybeProgrammingLanguage:  Option[ProgrammingLanguage]
  val successCodes:              List[SuccessCode]
  val commandParameterFactories: List[StepPlan => CommandParameterBase]

  override type PlanGroup         = StepPlan
  override type PlanGroupModified = StepPlan.Modified

  private lazy val commandParameters: List[CommandParameterBase] = commandParameterFactories.map(_.apply(this))
  lazy val parameters: List[CommandParameter] = commandParameters.collect { case param: CommandParameter => param }
  lazy val inputs:     List[CommandInput]     = commandParameters.collect { case in: CommandInput => in }
  lazy val outputs:    List[CommandOutput]    = commandParameters.collect { case out: CommandOutput => out }

  def getInput(location: Location): Option[CommandInput] = inputs.find(_.defaultValue.value == location)
}

object StepPlan {

  import io.renku.generators.Generators.Implicits._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  case class NonModified(id:                        Identifier,
                         name:                      Name,
                         maybeDescription:          Option[Description],
                         creators:                  List[Person],
                         dateCreated:               DateCreated,
                         keywords:                  List[Keyword],
                         maybeCommand:              Option[Command],
                         maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                         successCodes:              List[SuccessCode],
                         commandParameterFactories: List[StepPlan => CommandParameterBase]
  ) extends StepPlan
      with NonModifiedAlg {

    override type PlanType = StepPlan.NonModified
  }

  object NonModified {
    implicit def toEntitiesStepPlan(implicit renkuUrl: RenkuUrl): NonModified => entities.StepPlan.NonModified = plan =>
      entities.StepPlan.NonModified(
        plans.ResourceId(plan.asEntityId.show),
        plan.name,
        plan.maybeDescription,
        plan.creators.map(_.to[entities.Person]),
        plan.dateCreated,
        plan.keywords,
        plan.maybeCommand,
        plan.maybeProgrammingLanguage,
        plan.parameters.map(_.to[entities.CommandParameterBase.CommandParameter]),
        plan.inputs.map(_.to[entities.CommandParameterBase.CommandInput]),
        plan.outputs.map(_.to[entities.CommandParameterBase.CommandOutput]),
        plan.successCodes
      )
  }

  trait NonModifiedAlg extends PlanAlg {
    self: StepPlan.NonModified =>

    override def to[T](implicit convert: StepPlan.NonModified => T): T = convert(this)

    def createModification(f: StepPlan.NonModified => StepPlan.NonModified): StepPlan.Modified = {
      val modified = f(self)
      new Modified(
        planIdentifiers.generateOne,
        modified.name,
        modified.maybeDescription,
        modified.creators,
        modified.dateCreated,
        modified.keywords,
        modified.maybeCommand,
        modified.maybeProgrammingLanguage,
        modified.successCodes,
        modified.commandParameterFactories,
        parent = self
      )
    }

    override def invalidate(
        time: InvalidationTime
    ): ValidatedNel[String, StepPlan.Modified with HavingInvalidationTime] =
      validate(dateCreated, time).map(time =>
        new Modified(
          planIdentifiers.generateOne,
          name,
          maybeDescription,
          creators,
          dateCreated,
          keywords,
          maybeCommand,
          maybeProgrammingLanguage,
          successCodes,
          commandParameterFactories,
          parent = self
        ) with HavingInvalidationTime {
          override val invalidationTime: InvalidationTime = time
        }
      )

    override def replaceCreators(creators: List[Person]): StepPlan.NonModified = copy(creators = creators)

    override def replacePlanName(to: plans.Name): StepPlan.NonModified = copy(name = to)

    override def replaceCommand(to: Option[Command]): StepPlan.NonModified = copy(maybeCommand = to)

    override def replacePlanKeywords(to: List[plans.Keyword]): StepPlan.NonModified = copy(keywords = to)

    override def replacePlanDesc(to: Option[plans.Description]): StepPlan.NonModified = copy(maybeDescription = to)

    override def replacePlanDateCreated(to: plans.DateCreated): StepPlan.NonModified = copy(dateCreated = to)
  }

  case class Modified(id:                        Identifier,
                      name:                      Name,
                      maybeDescription:          Option[Description],
                      creators:                  List[Person],
                      dateCreated:               DateCreated,
                      keywords:                  List[Keyword],
                      maybeCommand:              Option[Command],
                      maybeProgrammingLanguage:  Option[ProgrammingLanguage],
                      successCodes:              List[SuccessCode],
                      commandParameterFactories: List[StepPlan => CommandParameterBase],
                      parent:                    StepPlan
  ) extends StepPlan
      with Plan.Modified
      with ModifiedAlg {
    override type ParentType = StepPlan
    override type PlanType   = StepPlan.Modified

    lazy val topmostParent: StepPlan = parent match {
      case p: NonModified => p
      case p: Modified    => p.topmostParent
    }
  }

  object Modified {
    implicit def toEntitiesStepPlan[P <: Modified](implicit renkuUrl: RenkuUrl): P => entities.StepPlan.Modified =
      plan => {
        val maybeInvalidationTime = plan match {
          case plan: StepPlan with HavingInvalidationTime => plan.invalidationTime.some
          case _ => None
        }

        entities.StepPlan.Modified(
          plans.ResourceId(plan.asEntityId.show),
          plan.name,
          plan.maybeDescription,
          plan.creators.map(_.to[entities.Person]),
          plan.dateCreated,
          plan.keywords,
          plan.maybeCommand,
          plan.maybeProgrammingLanguage,
          plan.parameters.map(_.to[entities.CommandParameterBase.CommandParameter]),
          plan.inputs.map(_.to[entities.CommandParameterBase.CommandInput]),
          plan.outputs.map(_.to[entities.CommandParameterBase.CommandOutput]),
          plan.successCodes,
          entities.Plan.Derivation(DerivedFrom(plan.parent.asEntityId),
                                   plans.ResourceId(plan.topmostParent.asEntityId.show)
          ),
          maybeInvalidationTime
        )
      }
  }

  trait ModifiedAlg extends PlanAlg {
    self: StepPlan.Modified =>

    override def to[T](implicit convert: StepPlan.Modified => T): T = convert(this)

    def createModification(f: StepPlan.Modified => StepPlan.Modified = identity): StepPlan.Modified = {
      val modified = f(self)
      if (modified.isInstanceOf[HavingInvalidationTime])
        throw new UnsupportedOperationException("Creating modification out of invalidated Plan not supported")
      new Modified(
        planIdentifiers.generateOne,
        modified.name,
        modified.maybeDescription,
        modified.creators,
        modified.dateCreated,
        modified.keywords,
        modified.maybeCommand,
        modified.maybeProgrammingLanguage,
        modified.successCodes,
        modified.commandParameterFactories,
        parent = self
      )
    }

    override def invalidate(
        time: InvalidationTime
    ): ValidatedNel[String, StepPlan.Modified with HavingInvalidationTime] =
      (validate(dateCreated, time), checkIfNotInvalidated)
        .mapN { (time, _) =>
          new Modified(
            planIdentifiers.generateOne,
            name,
            maybeDescription,
            creators,
            dateCreated,
            keywords,
            maybeCommand,
            maybeProgrammingLanguage,
            successCodes,
            commandParameterFactories,
            this
          ) with HavingInvalidationTime {
            override val invalidationTime: InvalidationTime = time
          }
        }

    private lazy val checkIfNotInvalidated: ValidatedNel[String, Unit] = this match {
      case _: HavingInvalidationTime =>
        Validated.invalidNel(show"Plan with id: $id is invalidated and cannot be invalidated again")
      case _ => Validated.valid(())
    }

    override def replaceCreators(creators: List[Person]): StepPlan.Modified = copy(creators = creators)

    override def replacePlanName(to: plans.Name): StepPlan.Modified = copy(name = to)

    override def replaceCommand(to: Option[Command]): StepPlan.Modified = copy(maybeCommand = to)

    override def replacePlanKeywords(to: List[plans.Keyword]): StepPlan.Modified = copy(keywords = to)

    override def replacePlanDesc(to: Option[plans.Description]): StepPlan.Modified = copy(maybeDescription = to)

    override def replacePlanDateCreated(to: plans.DateCreated): StepPlan.Modified = copy(dateCreated = to)
  }

  def of(name:                      Name,
         maybeCommand:              Option[Command],
         dateCreated:               DateCreated,
         creators:                  List[Person],
         commandParameterFactories: List[Position => StepPlan => CommandParameterBase]
  ): StepPlan.NonModified = StepPlan.NonModified(
    planIdentifiers.generateOne,
    name,
    maybeDescription = None,
    creators,
    dateCreated,
    keywords = Nil,
    maybeCommand = maybeCommand,
    maybeProgrammingLanguage = None,
    commandParameterFactories = commandParameterFactories.zipWithIndex.map { case (factory, idx) =>
      factory(Position(idx + 1))
    },
    successCodes = Nil
  )

  implicit def toEntitiesStepPlan(implicit renkuUrl: RenkuUrl): StepPlan => entities.StepPlan = {
    case p: StepPlan.NonModified => StepPlan.NonModified.toEntitiesStepPlan(renkuUrl)(p)
    case p: StepPlan.Modified    => StepPlan.Modified.toEntitiesStepPlan(renkuUrl)(p)
  }

  object CommandParameters {

    type CommandParameterFactory = Position => StepPlan => CommandParameterBase

    def of(parameters: CommandParameterFactory*): List[CommandParameterFactory] = parameters.toList
  }

  implicit def encoder(implicit renkuUrl: RenkuUrl, graphClass: GraphClass): JsonLDEncoder[StepPlan] =
    JsonLDEncoder.instance(_.to[entities.Plan].asJsonLD)

  implicit def entityIdEncoder[R <: Plan](implicit renkuUrl: RenkuUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => ResourceId(plan.id).asEntityId)
}
