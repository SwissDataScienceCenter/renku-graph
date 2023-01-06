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

package io.renku.graph.model.testentities

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.graph.model.{RenkuUrl, commandParameters, entities, parameterLinks}
import io.renku.graph.model.testentities.StepPlanCommandParameter.{CommandInput, CommandOutput, CommandParameter}
import io.renku.jsonld.{EntityIdEncoder, JsonLDEncoder}
import io.renku.jsonld.syntax._
import io.renku.tinytypes.constraints.UUID
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

case class ParameterLink(id: ParameterLink.Identifier, source: CommandOutput, sinks: NonEmptyList[ParameterLink.Sink])

object ParameterLink {

  final case class Identifier(value: String) extends StringTinyType

  object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with UUID[Identifier] {
    implicit def entityIdEncoder(implicit renkuUrl: RenkuUrl): EntityIdEncoder[Identifier] =
      EntityIdEncoder.instance(id => renkuUrl / "parameterLink" / id)
  }

  sealed trait Sink
  object Sink {
    final case class Input(input: CommandInput)     extends Sink
    final case class Param(param: CommandParameter) extends Sink

    def apply(input: CommandInput):     Sink = Input(input)
    def apply(param: CommandParameter): Sink = Param(param)

    implicit def entityIdEncoder(implicit renkuUrl: RenkuUrl): EntityIdEncoder[Sink] =
      EntityIdEncoder.instance {
        case Input(in) => in.asEntityId
        case Param(p)  => p.asEntityId
      }
  }

  def toEntitiesParameterLink(p: ParameterLink): entities.ParameterLink =
    entities.ParameterLink(
      resourceId = parameterLinks.ResourceId(p.id.asEntityId.show),
      source = commandParameters.ResourceId(p.source.asEntityId.show),
      sinks = p.sinks.map(s => commandParameters.ResourceId(s.asEntityId.show))
    )

  implicit def jsonLDEncoder: JsonLDEncoder[ParameterLink] =
    JsonLDEncoder.instance(toEntitiesParameterLink(_).asJsonLD)
}
