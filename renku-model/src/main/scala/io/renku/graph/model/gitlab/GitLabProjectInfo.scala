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

package io.renku.graph.model.gitlab

import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects._

final case class GitLabProjectInfo(
    id:               GitLabId,
    name:             Name,
    slug:             Slug,
    dateCreated:      DateCreated,
    dateModified:     DateModified,
    maybeDescription: Option[Description],
    maybeCreator:     Option[GitLabUser],
    keywords:         Set[Keyword],
    members:          Set[GitLabMember],
    visibility:       Visibility,
    maybeParentSlug:  Option[Slug],
    avatarUrl:        Option[ImageUri]
)

object GitLabProjectInfo {}
