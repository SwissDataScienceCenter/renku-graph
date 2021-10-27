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

package io.renku.config

import cats.MonadThrow
import io.renku.tinytypes.constraints.{BaseUrl, Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}

object renku {

  class ResourcesUrl private (val value: String) extends AnyVal with UrlTinyType
  object ResourcesUrl
      extends TinyTypeFactory[ResourcesUrl](new ResourcesUrl(_))
      with Url
      with BaseUrl[ResourcesUrl, ResourceUrl] {
    import ConfigLoader._
    import com.typesafe.config.{Config, ConfigFactory}
    import pureconfig.ConfigReader

    private implicit val configReader: ConfigReader[ResourcesUrl] = urlTinyTypeReader(this)

    def apply[F[_]: MonadThrow](
        config: Config = ConfigFactory.load()
    ): F[ResourcesUrl] =
      find[F, ResourcesUrl]("services.renku.resources-url", config)

  }

  class ResourceUrl private (val value: String) extends AnyVal with UrlTinyType
  implicit object ResourceUrl extends TinyTypeFactory[ResourceUrl](new ResourceUrl(_)) with Url with UrlOps[ResourceUrl]
}
