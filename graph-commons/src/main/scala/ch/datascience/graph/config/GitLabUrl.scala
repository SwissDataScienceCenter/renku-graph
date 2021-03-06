/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.config

import cats.MonadError
import ch.datascience.config.ConfigLoader.{find, stringTinyTypeReader}
import ch.datascience.tinytypes.constraints.{Url, UrlOps, UrlResourceRenderer}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

final class GitLabUrl private (val value: String) extends AnyVal with StringTinyType {
  def apiV4: GitLabApiUrl = GitLabApiUrl(this)
}
object GitLabUrl extends TinyTypeFactory[GitLabUrl](new GitLabUrl(_)) with Url with UrlOps[GitLabUrl] {

  private implicit val gitLabUrlReader: ConfigReader[GitLabUrl] = stringTinyTypeReader(GitLabUrl)

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[GitLabUrl] =
    find[Interpretation, GitLabUrl]("services.gitlab.url", config)
}

final class GitLabApiUrl private (val value: String) extends AnyVal with StringTinyType
object GitLabApiUrl
    extends TinyTypeFactory[GitLabApiUrl](new GitLabApiUrl(_))
    with Url
    with UrlOps[GitLabApiUrl]
    with UrlResourceRenderer[GitLabApiUrl] {
  def apply(gitLabUrl: GitLabUrl): GitLabApiUrl = new GitLabApiUrl((gitLabUrl / "api" / "v4").value)
}
