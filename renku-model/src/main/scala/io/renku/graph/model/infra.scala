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

package io.renku.graph.model

import cats.syntax.all._
import io.renku.graph.model.views.UrlResourceRenderer
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}

final class GitLabUrl private (val value: String) extends AnyVal with UrlTinyType {
  def apiV4: GitLabApiUrl = GitLabApiUrl(this)
}
object GitLabUrl extends TinyTypeFactory[GitLabUrl](new GitLabUrl(_)) with Url[GitLabUrl] with UrlOps[GitLabUrl] {
  override val transform: String => Either[Throwable, String] = {
    case v if v.endsWith("/") => v.substring(0, v.length - 1).asRight
    case v                    => v.asRight
  }
}

final class GitLabApiUrl private (val value: String) extends AnyVal with UrlTinyType
object GitLabApiUrl
    extends TinyTypeFactory[GitLabApiUrl](new GitLabApiUrl(_))
    with Url[GitLabApiUrl]
    with UrlOps[GitLabApiUrl]
    with UrlResourceRenderer[GitLabApiUrl] {
  def apply(gitLabUrl: GitLabUrl): GitLabApiUrl = new GitLabApiUrl((gitLabUrl / "api" / "v4").value)
}
