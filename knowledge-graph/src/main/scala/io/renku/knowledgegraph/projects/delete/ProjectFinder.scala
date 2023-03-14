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

package io.renku.knowledgegraph.projects.delete

import cats.effect.Async
import io.renku.events.consumers.Project
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient

private trait ProjectFinder[F[_]] {
  def findProject(path: projects.Path): F[Option[Project]]
}

private object ProjectFinder {
  def apply[F[_]: Async: GitLabClient]: ProjectFinder[F] = new ProjectFinderImpl[F]
}

private class ProjectFinderImpl[F[_]: Async: GitLabClient] extends ProjectFinder[F] {

  override def findProject(path: projects.Path): F[Option[Project]] = ???
}
