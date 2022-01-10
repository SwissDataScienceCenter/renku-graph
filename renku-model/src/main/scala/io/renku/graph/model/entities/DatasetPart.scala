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

package io.renku.graph.model.entities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.datasets.{DateCreated, PartExternal, PartResourceId, PartSource}

final case class DatasetPart(
    resourceId:            PartResourceId,
    external:              PartExternal,
    entity:                Entity,
    dateCreated:           DateCreated,
    maybeSource:           Option[PartSource],
    maybeInvalidationTime: Option[InvalidationTime]
)

object DatasetPart {

  def from(resourceId:            PartResourceId,
           external:              PartExternal,
           entity:                Entity,
           dateCreated:           DateCreated,
           maybeSource:           Option[PartSource],
           maybeInvalidationTime: Option[InvalidationTime]
  ): ValidatedNel[String, DatasetPart] =
    validate(maybeInvalidationTime, entity, dateCreated).map { _ =>
      DatasetPart(resourceId, external, entity, dateCreated, maybeSource, maybeInvalidationTime)
    }

  private def validate(
      maybeInvalidationTime: Option[InvalidationTime],
      entity:                Entity,
      dateCreated:           DateCreated
  ): ValidatedNel[String, Unit] =
    maybeInvalidationTime match {
      case Some(time) if (time.value compareTo dateCreated.value) < 0 =>
        s"DatasetPart ${entity.location} invalidationTime $time is older than DatasetPart ${dateCreated.value}".invalidNel
      case _ => ().validNel[String]
    }

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  private val entityTypes = EntityTypes of (prov / "Entity", schema / "DigitalDocument")

  implicit lazy val encoder: JsonLDEncoder[DatasetPart] = JsonLDEncoder.instance { part =>
    JsonLD.entity(
      part.resourceId.asEntityId,
      entityTypes,
      renku / "external"         -> part.external.asJsonLD,
      prov / "entity"            -> part.entity.asJsonLD,
      schema / "dateCreated"     -> part.dateCreated.asJsonLD,
      renku / "source"           -> part.maybeSource.asJsonLD,
      prov / "invalidatedAtTime" -> part.maybeInvalidationTime.asJsonLD
    )
  }

  implicit lazy val decoder: JsonLDDecoder[DatasetPart] = JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
    for {
      resourceId            <- cursor.downEntityId.as[PartResourceId]
      external              <- cursor.downField(renku / "external").as[PartExternal]
      entity                <- cursor.downField(prov / "entity").as[Entity]
      dateCreated           <- cursor.downField(schema / "dateCreated").as[DateCreated]
      maybeSource           <- cursor.downField(renku / "source").as[Option[PartSource]]
      maybeInvalidationTime <- cursor.downField(prov / "invalidatedAtTime").as[Option[InvalidationTime]]
      part <- DatasetPart
                .from(resourceId, external, entity, dateCreated, maybeSource, maybeInvalidationTime)
                .toEither
                .leftMap(errors => DecodingFailure(errors.intercalate("; "), Nil))
    } yield part
  }
}
