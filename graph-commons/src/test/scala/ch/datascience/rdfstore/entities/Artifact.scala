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

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.rdfstore.FusekiBaseUrl

sealed abstract class Artifact(val commitId: CommitId, val filePath: FilePath, val project: Project)

object Artifact {

  import cats.data.NonEmptyList
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def toProperties(
      entity:              Artifact
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NonEmptyList[(Property, JsonLD)] =
    NonEmptyList.of(
      rdfs / "label"      -> s"${entity.filePath}@${entity.commitId}".asJsonLD,
      schema / "isPartOf" -> entity.project.asJsonLD,
      prov / "atLocation" -> entity.filePath.asJsonLD
    )
}

final class ArtifactEntity private (val maybeFilePath:    Option[FilePath],
                                    val maybeCommitId:    Option[CommitId],
                                    val maybeGeneration:  Option[Generation],
                                    override val project: Project)
    extends Artifact(
      maybeCommitId orElse maybeGeneration.map(_.activity.id) getOrElse (throw new Exception(
        "No commitId for ArtifactEntity"
      )),
      maybeFilePath orElse maybeGeneration.map(_.filePath) getOrElse (throw new Exception(
        "No filePath for ArtifactEntity"
      )),
      project
    )

object ArtifactEntity {

  def apply(filePath: FilePath, generation: Generation): ArtifactEntity =
    new ArtifactEntity(Some(filePath), maybeCommitId = None, Some(generation), generation.activity.project)

  def apply(commitId: CommitId, filePath: FilePath, project: Project): ArtifactEntity =
    new ArtifactEntity(Some(filePath), Some(commitId), maybeGeneration = None, project)

  def apply(generation: Generation): ArtifactEntity =
    new ArtifactEntity(None, maybeCommitId = None, Some(generation), generation.activity.project)

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[ArtifactEntity] =
    JsonLDEncoder.instance { entity =>
      val commitId = entity.maybeCommitId orElse entity.maybeGeneration.map(_.activity.id) getOrElse (throw new Exception(
        "No commitId for ArtifactEntity"
      ))
      val filePath = entity.maybeFilePath orElse entity.maybeGeneration.map(_.filePath) getOrElse (throw new Exception(
        "No filePath for ArtifactEntity"
      ))
      JsonLD.entity(
        EntityId of fusekiBaseUrl / "blob" / commitId / filePath,
        EntityTypes of (wfprov / "Artifact", prov / "Entity"),
        Artifact.toProperties(entity),
        prov / "qualifiedGeneration" -> entity.maybeGeneration.asJsonLD
      )
    }
}

final case class ArtifactEntityCollection(override val commitId: CommitId,
                                          override val filePath: FilePath,
                                          override val project:  Project,
                                          members:               List[ArtifactEntity])
    extends Artifact(commitId, filePath, project)

object ArtifactEntityCollection {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl,
                       fusekiBaseUrl:         FusekiBaseUrl): JsonLDEncoder[ArtifactEntityCollection] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of fusekiBaseUrl / "blob" / entity.commitId / entity.filePath,
        EntityTypes of (wfprov / "Artifact", prov / "Entity", prov / "Collection"),
        Artifact.toProperties(entity),
        prov / "hadMember" -> entity.members.asJsonLD
      )
    }
}
