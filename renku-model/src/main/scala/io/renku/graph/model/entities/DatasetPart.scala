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

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.cli.model.CliDatasetFile
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

  def fromCli(cliPart: CliDatasetFile): ValidatedNel[String, DatasetPart] =
    from(cliPart.resourceId,
         cliPart.external,
         Entity.fromCli(cliPart.entity),
         cliPart.dateCreated,
         cliPart.source,
         cliPart.invalidationTime
    )

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

  import io.renku.graph.model.Schemas.{prov, renku, schema}
  import io.renku.jsonld._
  import io.renku.jsonld.ontology._
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

  implicit lazy val decoder: JsonLDDecoder[DatasetPart] =
    CliDatasetFile.jsonLDDecoder.emap { cliPart =>
      fromCli(cliPart).toEither.leftMap(_.intercalate("; "))
    }

  val ontology: Type = Type.Def(
    Class(schema / "DigitalDocument", ParentClass(prov / "Entity")),
    ObjectProperties(
      ObjectProperty(prov / "entity", Entity.ontology)
    ),
    DataProperties(
      DataProperty(renku / "external", xsd / "boolean"),
      DataProperty(schema / "dateCreated", xsd / "dateTime"),
      DataProperty(renku / "source", xsd / "string"),
      DataProperty(prov / "invalidatedAtTime", xsd / "dateTime")
    )
  )
}
