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

package io.renku.entities.searchgraphs

import io.renku.graph.model.entities.{Dataset, Project}

private object SearchInfoExtractor {

  def extractSearchInfo(project: Project)(datasets: List[Dataset[Dataset.Provenance]]): List[SearchInfo] =
    datasets.map { ds =>
      SearchInfo(
        ds.identification.resourceId,
        ds.provenance.topmostSameAs,
        ds.identification.name,
        project.visibility,
        ds.provenance.date,
        ds.provenance.creators,
        ds.additionalInfo.keywords,
        ds.additionalInfo.maybeDescription,
        ds.additionalInfo.images,
        List(project.resourceId)
      )
    }
}