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

import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.rdfstore.FusekiBaseUrl

final case class Usage(commitId: CommitId, commandInput: CommandInput, artifact: Artifact)

object Usage {

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Usage] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId of (fusekiBaseUrl / "activities" / "commit" / entity.commitId / "inputs" / entity.commandInput.entityIdToString),
        EntityTypes of (prov / "Usage"),
        prov / "entity"  -> toJsonLD(entity.artifact),
        prov / "hadRole" -> entity.commandInput.entityIdToString.asJsonLD
      )
    }

  private def toJsonLD(
      artifact:            Artifact
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD = artifact match {
    case a: ArtifactEntity           => a.asJsonLD
    case a: ArtifactEntityCollection => a.asJsonLD
  }
}
