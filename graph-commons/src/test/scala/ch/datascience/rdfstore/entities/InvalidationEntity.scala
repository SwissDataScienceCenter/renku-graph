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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.rdfstore.FusekiBaseUrl
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, JsonLDEncoder}

final case class InvalidationEntity(
    override val commitId: CommitId,
    entityIdToInvalidate:  String,
    override val project:  Project,
    invalidationActivity:  Activity
) extends Entity(commitId,
                 Location(".renku") / "datasets" / entityIdToInvalidate / "metadata.yml",
                 project,
                 Some(invalidationActivity),
                 None
    )
    with Artifact

object InvalidationEntity {

  private[entities] implicit def converter(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): PartialEntityConverter[InvalidationEntity] =
    new PartialEntityConverter[InvalidationEntity] {
      override def convert[T <: InvalidationEntity]: T => Either[Exception, PartialEntity] =
        entity => PartialEntity(schema / "isPartOf" -> entity.project.asEntityId.asJsonLD).asRight

      override def toEntityId: InvalidationEntity => Option[EntityId] =
        entity => (EntityId of fusekiBaseUrl / "blob" / entity.commitId / entity.location).some
    }

  implicit def encoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[InvalidationEntity] =
    JsonLDEncoder.instance { entity =>
      entity
        .asPartialJsonLD[Entity]
        .combine(entity.asPartialJsonLD[InvalidationEntity])
        .combine(entity.asPartialJsonLD[Artifact])
        .getOrFail
    }

}
