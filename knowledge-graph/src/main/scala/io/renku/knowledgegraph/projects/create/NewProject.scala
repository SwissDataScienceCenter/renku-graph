/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.create

import io.renku.core.client.{Branch, Template}
import io.renku.graph.model.projects
import io.renku.knowledgegraph.projects.images.Image

private final case class NewProject(
    name:             projects.Name,
    namespace:        Namespace,
    maybeDescription: Option[projects.Description],
    keywords:         Set[projects.Keyword],
    visibility:       projects.Visibility,
    template:         Template,
    maybeImage:       Option[Image]
) {
  val branch: Branch = Branch.default
}

private sealed trait Namespace {
  val identifier: NamespaceId
}

private object Namespace {

  def apply(identifier: NamespaceId): Namespace.Id = Namespace.Id(identifier)

  final case class Id(identifier: NamespaceId) extends Namespace {
    def withName(name: projects.Namespace): Namespace.WithName = Namespace.WithName(identifier, name)
  }
  final case class WithName(identifier: NamespaceId, name: projects.Namespace) extends Namespace
}
