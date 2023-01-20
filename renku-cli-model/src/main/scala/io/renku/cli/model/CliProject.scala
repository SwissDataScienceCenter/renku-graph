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

package io.renku.cli.model

import io.renku.graph.model.images.Image
import io.renku.graph.model.projects._

final case class CliProject(
    id:          ResourceId,
    path:        Path,
    name:        Name,
    description: Option[Description],
    dateCreated: DateCreated,
    creator:     Option[CliPerson],
    visibility:  Visibility,
    keywords:    Set[Keyword],
    members:     Set[CliPerson],
    images:      List[Image]
) extends CliModel

object CliProject {}
