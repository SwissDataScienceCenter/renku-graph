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
import io.renku.graph.model._
import io.renku.graph.model.commandParameters.{Description, Name}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityIdEncoder, JsonLDEncoder}
import io.renku.tinytypes.constraints.UUID
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

final case class ParameterMapping(
    id:           ParameterMapping.Identifier,
    name:         Name,
    description:  Option[Description],
    defaultValue: Option[String],
    planId:       plans.Identifier,
    mappedParam:  NonEmptyList[CommandParameterBase]
) extends CommandParameterBase {
  override type DefaultValue = Option[String]
  override val maybePrefix: Option[commandParameters.Prefix] = None
}

object ParameterMapping {

  final case class Identifier(value: String) extends StringTinyType

  object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with UUID[Identifier] {
    implicit def entityIdEncoder(implicit renkuUrl: RenkuUrl): EntityIdEncoder[Identifier] =
      EntityIdEncoder.instance(id => renkuUrl / "parameterMapping" / id)
  }

  def toEntitiesParameterMapping(m: ParameterMapping)(implicit renkuUrl: RenkuUrl): entities.ParameterMapping =
    entities.ParameterMapping(
      resourceId = commandParameters.ResourceId(m.id.asEntityId.show),
      defaultValue = m.defaultValue.map(entities.ParameterMapping.DefaultValue),
      maybeDescription = m.description,
      name = m.name,
      mappedParameter = m.mappedParam.map(c => commandParameters.ResourceId(c.asEntityId.show))
    )

  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ParameterMapping] =
    JsonLDEncoder.instance(toEntitiesParameterMapping(_).asJsonLD)
}
