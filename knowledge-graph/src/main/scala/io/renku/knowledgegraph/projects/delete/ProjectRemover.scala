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
import io.renku.graph.model.projects
import io.renku.http.client.GitLabClient

private trait ProjectRemover[F[_]] {
  def deleteProject(id: projects.GitLabId): F[Unit]
}

private object ProjectRemover {
  def apply[F[_]: Async: GitLabClient]: ProjectRemover[F] = new ProjectRemoverImpl[F]
}

private class ProjectRemoverImpl[F[_]: Async: GitLabClient] extends ProjectRemover[F] {

  override def deleteProject(id: projects.GitLabId): F[Unit] = ???
}
