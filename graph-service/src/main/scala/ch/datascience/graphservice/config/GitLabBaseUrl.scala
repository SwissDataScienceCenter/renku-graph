/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graphservice.config

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.find
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import ch.datascience.tinytypes.constraints.{Url, UrlOps}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.language.higherKinds

class GitLabBaseUrl private (val value: String) extends AnyVal with TinyType[String]
object GitLabBaseUrl
    extends TinyTypeFactory[String, GitLabBaseUrl](new GitLabBaseUrl(_))
    with Url
    with UrlOps[GitLabBaseUrl] {

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[GitLabBaseUrl] =
    find[Interpretation, GitLabBaseUrl]("services.gitlab.url", config)

  private implicit val gitLabBaseUrlReader: ConfigReader[GitLabBaseUrl] =
    ConfigReader.fromString[GitLabBaseUrl] { value =>
      GitLabBaseUrl
        .from(value)
        .leftMap(exception => CannotConvert(value, FusekiBaseUrl.getClass.toString, exception.getMessage))
    }
}
