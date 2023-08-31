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

package io.renku.projectauth.util

import fs2.Pipe
import io.circe.Decoder
import io.renku.graph.model.projects.{Slug, Visibility}
import io.renku.projectauth.{ProjectAuthData, ProjectMember}
import io.renku.triplesstore.client.http.RowDecoder
import io.renku.tinytypes.json.TinyTypeDecoders._

final case class ProjectAuthDataRow(slug: Slug, visibility: Visibility, memberRole: Option[ProjectMember])

object ProjectAuthDataRow {

  private implicit val projectMemberDecoder: Decoder[ProjectMember] =
    Decoder.decodeString.emap(ProjectMember.fromEncoded)

  implicit val tupleRowDecoder: RowDecoder[ProjectAuthDataRow] =
    RowDecoder.forProduct3("slug", "visibility", "memberRole")(ProjectAuthDataRow.apply)

  def collect[F[_]]: Pipe[F, ProjectAuthDataRow, ProjectAuthData] =
    _.groupAdjacentBy(_.slug)
      .map { case (slug, rest) =>
        val members = rest.toList.flatMap(_.memberRole)
        val vis     = rest.head.map(_.visibility)
        vis.map(v => ProjectAuthData(slug, members.toSet, v))
      }
      .unNone
}
