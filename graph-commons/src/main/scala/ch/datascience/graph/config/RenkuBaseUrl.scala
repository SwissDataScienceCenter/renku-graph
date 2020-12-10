/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.config.ConfigLoader
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.tinytypes.constraints.{Url, UrlOps, UrlResourceRenderer}
import ch.datascience.tinytypes.{Renderer, StringTinyType, TinyTypeFactory}

class RenkuBaseUrl private (val value: String) extends AnyVal with StringTinyType
object RenkuBaseUrl
    extends TinyTypeFactory[RenkuBaseUrl](new RenkuBaseUrl(_))
    with Url
    with UrlOps[RenkuBaseUrl]
    with UrlResourceRenderer[RenkuBaseUrl] {

  import ConfigLoader._
  import com.typesafe.config.{Config, ConfigFactory}
  import pureconfig.ConfigReader

  private implicit val renkuBaseUrlReader: ConfigReader[RenkuBaseUrl] = stringTinyTypeReader(this)

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[RenkuBaseUrl] =
    find[Interpretation, RenkuBaseUrl]("services.renku.url", config)
}
