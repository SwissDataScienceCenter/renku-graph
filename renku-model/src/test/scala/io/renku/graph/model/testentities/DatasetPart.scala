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

import cats.syntax.all._
import io.renku.graph.model.datasets.{DateCreated, PartExternal, PartId, PartSource}
import io.renku.graph.model.{datasets, entities}

case class DatasetPart(
    id:          PartId,
    external:    PartExternal,
    entity:      Entity,
    dateCreated: DateCreated,
    maybeSource: Option[PartSource]
)

object DatasetPart {

  import io.renku.graph.model.RenkuUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def toEntitiesDatasetPart(implicit renkuUrl: RenkuUrl): DatasetPart => entities.DatasetPart = {
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

  implicit def encoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[DatasetPart] =
    JsonLDEncoder.instance(_.to[entities.DatasetPart].asJsonLD)

  implicit def entityIdEncoder[D <: DatasetPart](implicit renkuUrl: RenkuUrl): EntityIdEncoder[D] =
    EntityIdEncoder.instance(part => renkuUrl / "dataset-files" / part.id / part.entity.location)
}
