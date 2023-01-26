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

import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.datasets._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliDatasetFile(
    resourceId:       PartResourceId,
    external:         PartExternal,
    entity:           CliEntity,
    dateCreated:      DateCreated,
    source:           Option[PartSource],
    invalidationTime: Option[InvalidationTime]
) extends CliModel

object CliDatasetFile {
  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Entity, Schema.DigitalDocument)

  implicit def jsonLDDecoder: JsonLDDecoder[CliDatasetFile] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        resourceId            <- cursor.downEntityId.as[PartResourceId]
        external              <- cursor.downField(Renku.external).as[PartExternal]
        entity                <- cursor.downField(Prov.entity).as[CliEntity]
        dateCreated           <- cursor.downField(Schema.dateCreated).as[DateCreated]
        maybeSource           <- cursor.downField(Renku.source).as[Option[PartSource]]
        maybeInvalidationTime <- cursor.downField(Prov.invalidatedAtTime).as[Option[InvalidationTime]]
        part = CliDatasetFile(resourceId, external, entity, dateCreated, maybeSource, maybeInvalidationTime)
      } yield part
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliDatasetFile] = JsonLDEncoder.instance { file =>
    JsonLD.entity(
      file.resourceId.asEntityId,
      entityTypes,
      List(
        Some(Renku.external     -> file.external.asJsonLD),
        Some(Prov.entity        -> file.entity.asJsonLD),
        Some(Schema.dateCreated -> file.dateCreated.asJsonLD),
        file.source.map(s => Renku.source -> s.asJsonLD),
        file.invalidationTime.map(t => Prov.invalidatedAtTime -> t.asJsonLD)
      ).flatten.toMap
    )
  }
}
