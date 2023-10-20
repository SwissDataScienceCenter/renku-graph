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

package io.renku.entities.searchgraphs.datasets

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.{Dataset, Project}

private object ModelSearchInfoExtractor {

  def extractModelSearchInfos[F[_]: MonadThrow](
      project: Project
  )(datasets: List[Dataset[Dataset.Provenance]]): F[List[ModelDatasetSearchInfo]] =
    datasets
      .map(toSearchInfo[F](project))
      .sequence

  private def toSearchInfo[F[_]: MonadThrow](project: Project)(ds: Dataset[Dataset.Provenance]) = ds.provenance match {
    case prov: Dataset.Provenance.Modified =>
      findDateOriginal(prov, project)
        .map(createSearchInfo(ds, _, datasets.DateModified(prov.date).some, project))
    case prov =>
      createSearchInfo(ds, prov.date, maybeDateModified = None, project).pure[F]
  }

  private def findDateOriginal[F[_]: MonadThrow](prov:    Dataset.Provenance.Modified,
                                                 project: Project
  ): F[datasets.CreatedOrPublished] =
    project.datasets
      .find(_.identification.resourceId.value == prov.topmostDerivedFrom.value)
      .map(_.provenance.date.pure[F].widen[datasets.CreatedOrPublished])
      .getOrElse(
        new Exception(
          show"Cannot find original Dataset ${prov.topmostDerivedFrom} for project ${project.resourceId}"
        ).raiseError[F, datasets.CreatedOrPublished]
      )
      .widen

  private def createSearchInfo(ds:                 Dataset[Dataset.Provenance],
                               createdOrPublished: datasets.CreatedOrPublished,
                               maybeDateModified:  Option[datasets.DateModified],
                               project:            Project
  ) = ModelDatasetSearchInfo(
    ds.provenance.topmostSameAs,
    ds.identification.name,
    createdOrPublished,
    maybeDateModified,
    ds.provenance.creators.map(Creator.from),
    ds.additionalInfo.keywords,
    ds.additionalInfo.maybeDescription,
    ds.additionalInfo.images,
    Link.from(ds.provenance.topmostSameAs,
              ds.identification.resourceId,
              project.resourceId,
              project.slug,
              project.visibility
    )
  )
}
