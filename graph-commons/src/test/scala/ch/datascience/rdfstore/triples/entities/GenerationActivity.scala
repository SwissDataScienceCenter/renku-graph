/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.rdfstore.FusekiBaseUrl
import io.circe.Json
import io.circe.literal._

private[triples] object GenerationActivity {

  def apply(commitId: CommitId, filePath: FilePath, activityId: EntityId)(implicit fusekiBaseUrl: FusekiBaseUrl): Json =
    apply(Id(commitId, filePath), activityId)

  def apply(id: Id, activityId: EntityId): Json = json"""
  {
    "@id": $id,
    "@type": "http://www.w3.org/ns/prov#Generation",
    "http://www.w3.org/ns/prov#activity": {
      "@id": $activityId
    }
  }"""

  final case class Id(commitId: CommitId, filePath: FilePath)(implicit fusekiBaseUrl: FusekiBaseUrl) extends EntityId {
    override val value: String = (fusekiBaseUrl / "commit" / commitId / filePath).toString
  }
}
