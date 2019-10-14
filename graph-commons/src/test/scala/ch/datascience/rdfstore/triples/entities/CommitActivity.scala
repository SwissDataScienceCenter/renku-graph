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

import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.triples.entities.Project.`schema:isPartOf`
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.Json
import io.circe.literal._

private[triples] object CommitActivity {

  def apply(commitId:          CommitId,
            projectId:         Project.Id,
            committedDate:     CommittedDate,
            agentId:           Agent.Id,
            personId:          Person.Id,
            maybeInfluencedBy: List[CommitCollectionEntity.Id],
            comment:           String Refined NonEmpty)(implicit fusekiBaseUrl: FusekiBaseUrl): Json =
    apply(Id(commitId), projectId, committedDate, agentId, personId, maybeInfluencedBy, comment)

  // format: off
  def apply(id:                Id,
            projectId:         Project.Id,
            committedDate:     CommittedDate,
            agentId:           Agent.Id,
            personId:          Person.Id,
            maybeInfluencedBy: List[CommitCollectionEntity.Id] = Nil,
            comment:           String Refined NonEmpty = "some change"
  ): Json = json"""
  {
    "@id": $id,
    "@type": "prov:Activity",
    "prov:endedAtTime": {
      "@type": "xsd:dateTime",
      "@value": "2018-12-06T11:26:33+01:00"
    },
    "prov:startedAtTime": {
      "@type": "xsd:dateTime",
      "@value": ${committedDate.value}
    },
    "prov:wasInformedBy": {
      "@id": $id
    },
    "rdfs:comment": ${comment.value},
    "rdfs:label": ${id.commitId.value},
    "prov:agent": [${agentId.toIdJson}, ${personId.toIdJson}]
  }""".deepMerge(`schema:isPartOf`(projectId))
      .deepMerge(maybeInfluencedBy toResources "prov:influenced")
  // format: on

  final case class Id(commitId: CommitId)(implicit fusekiBaseUrl: FusekiBaseUrl) extends EntityId {
    override val value: String = (fusekiBaseUrl / "commit" / commitId).toString
  }
}
