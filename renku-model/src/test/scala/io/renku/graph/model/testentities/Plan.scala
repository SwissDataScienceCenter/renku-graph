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
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.Position
import io.renku.graph.model.plans._

trait Plan extends PlanAlg {
  val id:               Identifier
  val name:             Name
  val maybeDescription: Option[Description]
  val creators:         List[Person]
  val dateCreated:      DateCreated
  val keywords:         List[Keyword]
  type PlanGroup <: Plan
  type PlanGroupModified <: PlanGroup with Plan.Modified
  type PlanType <: PlanGroup
}

trait PlanAlg { self: Plan =>

  def to[T](implicit convert: PlanType => T): T

  def createModification(f: PlanType => PlanType): PlanGroupModified

  def invalidate(): PlanGroupModified with HavingInvalidationTime = invalidate(
    timestampsNotInTheFuture(butYoungerThan = this.dateCreated.value).generateAs(InvalidationTime)
  ).fold(err => throw new Exception(s"Save invalidate failed: ${err.intercalate("; ")}"), identity)

  def invalidate(time: InvalidationTime): ValidatedNel[String, PlanGroupModified with HavingInvalidationTime]

  protected def validate(dateCreated:      plans.DateCreated,
                         invalidationTime: InvalidationTime
  ): ValidatedNel[String, InvalidationTime] =
    Validated.condNel(
      test = (invalidationTime.value compareTo dateCreated.value) >= 0,
      invalidationTime,
      show"Invalidation time $invalidationTime on StepPlan with id: $id is older than dateCreated"
    )

  def replaceCreators(creators: List[Person]): PlanType

  def removeCreators(): PlanType = replaceCreators(Nil)

  def replacePlanName(to: plans.Name): PlanType

  def replaceCommand(to: Option[Command]): PlanType

  def replacePlanKeywords(to: List[plans.Keyword]): PlanType

  def replacePlanDesc(to: Option[plans.Description]): PlanType

  def replacePlanDateCreated(to: plans.DateCreated): PlanType
}

object Plan {

  trait Modified { self: Plan =>
    type ParentType <: Plan
    val parent: ParentType
  }

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def of(name:                      Name,
         maybeCommand:              Option[Command],
         dateCreated:               DateCreated,
         creators:                  List[Person],
         commandParameterFactories: List[Position => StepPlan => CommandParameterBase]
  ): StepPlan.NonModified = StepPlan.of(name, maybeCommand, dateCreated, creators, commandParameterFactories)

  implicit def toEntitiesPlan[P <: Plan](implicit renkuUrl: RenkuUrl): P => entities.Plan = {
    case p: StepPlan.NonModified => p.to[entities.Plan](StepPlan.NonModified.toEntitiesStepPlan)
    case p: StepPlan.Modified    => p.to[entities.Plan](StepPlan.Modified.toEntitiesStepPlan)
  }

  implicit def encoder[P <: Plan](implicit renkuUrl: RenkuUrl, graphClass: GraphClass): JsonLDEncoder[P] =
    JsonLDEncoder.instance(_.to[entities.Plan].asJsonLD)

  implicit def entityIdEncoder[R <: Plan](implicit renkuUrl: RenkuUrl): EntityIdEncoder[R] =
    EntityIdEncoder.instance(plan => ResourceId(plan.id).asEntityId)
}
