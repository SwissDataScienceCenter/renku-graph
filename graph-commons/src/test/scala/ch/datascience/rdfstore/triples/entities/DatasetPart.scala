/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.triples
package entities

import ch.datascience.graph.model.datasets.{PartLocation, PartName}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.triples.entities.Project.`schema:isPartOf`
import io.circe.Json
import io.circe.literal._

object DatasetPart {

  def apply(id: Id, name: PartName, projectId: Project.Id): Json = json"""
    {
      "@id": $id,
      "@type": [
        "http://www.w3.org/ns/prov#Entity",
        "http://schema.org/DigitalDocument",
        "http://purl.org/wf4ever/wfprov#Artifact"
      ],
      "http://www.w3.org/ns/prov#atLocation": ${id.location.toString},
      "http://www.w3.org/2000/01/rdf-schema#label": ${s"${id.location}@${id.commitId}"},
      "http://schema.org/name": ${name.toString}
    }
  """.deepMerge(`schema:isPartOf`(projectId))

  final case class Id(commitId: CommitId, location: PartLocation)(implicit fusekiBaseUrl: FusekiBaseUrl)
      extends EntityId {
    override val value: String = (fusekiBaseUrl / "blob" / commitId / location).toString
  }
}
