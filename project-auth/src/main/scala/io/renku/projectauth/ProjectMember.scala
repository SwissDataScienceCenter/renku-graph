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

import io.renku.graph.model.Schemas
import io.renku.graph.model.persons.GitLabId
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.triplesstore.client.http.RowDecoder
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class ProjectMember(
    gitLabId: GitLabId,
    role:     Role
)

object ProjectMember {

  def fromGitLabData(gitLabId: GitLabId, accessLevel: Int): ProjectMember =
    ProjectMember(gitLabId, Role.fromGitLabAccessLevel(accessLevel))

  implicit def jsonLDEncoder: JsonLDEncoder[ProjectMember] =
    JsonLDEncoder.instance { member =>
      JsonLD.entity(
        EntityId.blank,
        EntityTypes.of(Schemas.schema / "Member"),
        Schemas.schema / "identifier" -> member.gitLabId.asJsonLD,
        Schemas.schema / "role"       -> member.role.asString.asJsonLD
      )
    }

  implicit def rowDecoder: RowDecoder[ProjectMember] =
    RowDecoder.forProduct2("gitLabId", "role")(ProjectMember.apply)
}
