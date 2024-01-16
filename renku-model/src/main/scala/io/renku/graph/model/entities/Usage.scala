/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.cli.model.CliUsage
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.usages.ResourceId
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.ontology._
import io.renku.jsonld.syntax._

final case class Usage(resourceId: ResourceId, entity: Entity)

object Usage {

  val entityTypes: EntityTypes = EntityTypes of (prov / "Usage")

  implicit lazy val encoder: JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { case Usage(resourceId, entity) =>
      JsonLD.entity(
        resourceId.asEntityId,
        entityTypes,
        prov / "entity" -> entity.asJsonLD
      )
    }

  def fromCli(cliUsage: CliUsage): ValidatedNel[String, Usage] =
    Usage(cliUsage.resourceId, Entity.fromCli(cliUsage.entity)).validNel

  lazy val ontology: Type = Type.Def(
    Class(prov / "Usage"),
    ObjectProperty(prov / "entity", Entity.ontology)
  )
}
