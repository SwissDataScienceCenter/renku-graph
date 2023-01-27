/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import Ontologies.{Prov, Schema}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.jsonld._
import io.renku.jsonld.syntax._

sealed trait CliPlan extends CliModel {
  def fold[A](fa: CliStepPlan => A, fb: CliCompositePlan => A): A
}

object CliPlan {
  final case class Step(value: CliStepPlan) extends CliPlan {
    def fold[A](fa: CliStepPlan => A, fb: CliCompositePlan => A): A = fa(value)
  }

  final case class Composite(value: CliCompositePlan) extends CliPlan {
    def fold[A](fa: CliStepPlan => A, fb: CliCompositePlan => A): A = fb(value)
  }

  def apply(value: CliStepPlan): CliPlan = Step(value)

  def apply(value: CliCompositePlan): CliPlan = Composite(value)

  private val entityTypes: EntityTypes =
    EntityTypes.of(Prov.Plan, Schema.Action, Schema.CreativeWork)

  private def selectCandidates(ets: EntityTypes): Boolean =
    CliStepPlan.matchingEntityTypes(ets) || CliCompositePlan.matchingEntityTypes(ets)

  implicit def jsonLDDecoder: JsonLDDecoder[CliPlan] = {
    val plan = CliStepPlan.jsonLDDecoder.emap(plan => CliPlan(plan).asRight)
    val cp   = CliCompositePlan.jsonLDDecoder.emap(plan => CliPlan(plan).asRight)

    JsonLDDecoder.cacheableEntity(entityTypes, _.getEntityTypes.map(selectCandidates)) { cursor =>
      val currentEntityTypes = cursor.getEntityTypes
      (currentEntityTypes.map(CliStepPlan.matchingEntityTypes),
       currentEntityTypes.map(CliCompositePlan.matchingEntityTypes)
      ).flatMapN {
        case (true, _) => plan(cursor)
        case (_, true) => cp(cursor)
        case _ =>
          DecodingFailure(
            s"Invalid entity for decoding child plans of a composite plan: $currentEntityTypes",
            Nil
          ).asLeft
      }
    }
  }

  implicit def jsonLDEncoder: JsonLDEncoder[CliPlan] =
    JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
}
