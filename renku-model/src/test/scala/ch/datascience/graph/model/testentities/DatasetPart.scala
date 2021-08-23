/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.testentities

import cats.syntax.all._
import ch.datascience.graph.model.datasets.{DateCreated, PartExternal, PartId, PartSource}
import ch.datascience.graph.model.{datasets, entities}

case class DatasetPart(
    id:          PartId,
    external:    PartExternal,
    entity:      Entity,
    dateCreated: DateCreated,
    maybeSource: Option[PartSource]
)

object DatasetPart {

  import ch.datascience.graph.model.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesDatasetPart(implicit renkuBaseUrl: RenkuBaseUrl): DatasetPart => entities.DatasetPart = {
    case datasetPart: DatasetPart with HavingInvalidationTime =>
      entities.DatasetPart(
        datasets.PartResourceId(datasetPart.asEntityId.show),
        datasetPart.external,
        datasetPart.entity.to[entities.Entity],
        datasetPart.dateCreated,
        datasetPart.maybeSource,
        datasetPart.invalidationTime.some
      )
    case datasetPart =>
      entities.DatasetPart(
        datasets.PartResourceId(datasetPart.asEntityId.show),
        datasetPart.external,
        datasetPart.entity.to[entities.Entity],
        datasetPart.dateCreated,
        datasetPart.maybeSource,
        None
      )
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[DatasetPart] =
    JsonLDEncoder.instance(_.to[entities.DatasetPart].asJsonLD)

  implicit def entityIdEncoder[D <: DatasetPart](implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[D] =
    EntityIdEncoder.instance(part => renkuBaseUrl / "dataset-files" / part.id / part.entity.location)
}
