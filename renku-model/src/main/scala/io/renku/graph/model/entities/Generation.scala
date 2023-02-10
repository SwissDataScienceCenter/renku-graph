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

package io.renku.graph.model.entities

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.cli.model.CliGeneration
import io.renku.graph.model.Schemas.prov
import io.renku.graph.model.entities.Entity.OutputEntity
import io.renku.graph.model.generations.ResourceId
import io.renku.graph.model.activities
import io.renku.jsonld._
import io.renku.jsonld.ontology._
import io.renku.jsonld.syntax.JsonEncoderOps

final case class Generation(resourceId: ResourceId, activityResourceId: activities.ResourceId, entity: OutputEntity)

object Generation {

  private val entityTypes: EntityTypes = EntityTypes of prov / "Generation"

  implicit lazy val encoder: JsonLDEncoder[Generation] =
    JsonLDEncoder.instance { generation =>
      JsonLD.entity(
        generation.resourceId.asEntityId,
        entityTypes,
        Reverse.ofJsonLDsUnsafe(prov / "qualifiedGeneration" -> generation.entity.asJsonLD),
        prov / "activity" -> generation.activityResourceId.asEntityId.asJsonLD
      )
    }

  def fromCli(cliGen: CliGeneration): ValidatedNel[String, Generation] = {
    val entity =
      Entity.fromCli(cliGen.entity) match {
        case e: Entity.OutputEntity => e.validNel
        case e => Validated.invalidNel(s"Expected output entity for a Generation, but got: $e")
      }
    entity.map { e =>
      Generation(
        cliGen.resourceId,
        cliGen.activityResourceId,
        e
      )
    }
  }

  def decoder(activityId: activities.ResourceId): JsonLDDecoder[Generation] =
    CliGeneration.decoderForActivity(activityId).emap { cliGen =>
      fromCli(cliGen).toEither.leftMap(_.intercalate("; "))
    }

  lazy val ontology: Type = Type.Def(
    Class(prov / "Generation"),
    ObjectProperty(prov / "activity", Activity.ontologyClass)
  )
}
