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
import io.renku.graph.model.commandParameters
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

  lazy val allStepParameterIds: CliStepPlan => Set[commandParameters.ResourceId] = p =>
    (p.parameters ::: p.inputs ::: p.outputs).map(_.resourceId).toSet

  lazy val allMappingParameterIds: CliCompositePlan => Set[commandParameters.ResourceId] = p =>
    p.mappings.map(_.resourceId).toSet

  private val entityTypes: EntityTypes =
    EntityTypes.of(Prov.Plan, Schema.Action, Schema.CreativeWork)

  implicit def jsonLDDecoder: JsonLDEntityDecoder[CliPlan] =
    jsonLDDecoderWith(CliStepPlan.jsonLDDecoder, CliCompositePlan.jsonLDDecoder)

  /** Decodes also "subtypes" of step plans, i.e. the WorkflowFilePlan, viewing them as a step/composite plan. */
  def jsonLDDecoderLenientTyped: JsonLDEntityDecoder[CliPlan] =
    jsonLDDecoderWith(CliStepPlan.jsonLDDecoderLenientTyped, CliCompositePlan.jsonLDDecoderLenientTyped)

  private def jsonLDDecoderWith(
      stepPlanDecoder: JsonLDEntityDecoder[CliStepPlan],
      compPlanDecoder: JsonLDEntityDecoder[CliCompositePlan]
  ): JsonLDEntityDecoder[CliPlan] = {
    val step = stepPlanDecoder.map(CliPlan.apply)
    val comp = compPlanDecoder.map(CliPlan.apply)
    val predicate = (cursor: Cursor) =>
      (stepPlanDecoder.predicate(cursor), compPlanDecoder.predicate(cursor)).mapN(_ || _)

    JsonLDDecoder.cacheableEntity(entityTypes, predicate) { cursor =>
      val currentEntityTypes = cursor.getEntityTypes
      (stepPlanDecoder.predicate(cursor), compPlanDecoder.predicate(cursor)).flatMapN {
        case (true, _) => step(cursor)
        case (_, true) => comp(cursor)
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
