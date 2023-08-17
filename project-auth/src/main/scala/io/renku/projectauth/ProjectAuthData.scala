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

package io.renku.projectauth

import io.renku.graph.model.projects.{ResourceId, Slug, Visibility}
import io.renku.graph.model.{RenkuUrl, Schemas}
import io.renku.jsonld.JsonLD.JsonLDArray
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

final case class ProjectAuthData(
    path:       Slug,
    members:    Set[ProjectMember],
    visibility: Visibility
)

object ProjectAuthData {
  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ProjectAuthData] =
    JsonLDEncoder.instance { data =>
      JsonLD.entity(
        ResourceId(data.path).asEntityId,
        EntityTypes.of(Schemas.schema / "Project"),
        Schemas.renku / "slug"       -> data.path.asJsonLD,
        Schemas.renku / "visibility" -> data.visibility.asJsonLD,
        Schemas.renku / "memberId"   -> JsonLDArray(data.members.map(_.gitLabId.asJsonLD).toSeq),
        Schemas.renku / "memberRole" -> JsonLDArray(data.members.map(_.encoded.asJsonLD).toSeq)
      )
    }
}
