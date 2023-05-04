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

import cats.syntax.all._
import io.renku.graph.model.datasets.DerivedFrom
import io.renku.graph.model.entities.{Dataset, Project}

private object DatasetsCollector {

  lazy val collectLastVersions: Project => List[Dataset[Dataset.Provenance]] = project => {
    val derives = project.datasets.flatMap(ds => findDerivedFrom(ds.provenance))

    project.datasets
      .filter(dsWithoutModifications(derives))
      .filter(invalidatedDS)
  }

  private lazy val findDerivedFrom: Dataset.Provenance => Option[DerivedFrom] = {
    case prov: Dataset.Provenance.Modified => prov.derivedFrom.some
    case _ => None
  }

  private def dsWithoutModifications(derives: List[DerivedFrom])(ds: Dataset[Dataset.Provenance]): Boolean =
    !derives.exists(_.value == ds.identification.resourceId.value)

  private lazy val invalidatedDS: Dataset[Dataset.Provenance] => Boolean = _.provenance match {
    case prov: Dataset.Provenance.Modified => prov.maybeInvalidationTime.isEmpty
    case _ => true
  }
}
