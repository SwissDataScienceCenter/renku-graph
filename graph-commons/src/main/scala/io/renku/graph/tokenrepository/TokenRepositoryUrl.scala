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

package io.renku.graph.tokenrepository

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.{find, urlTinyTypeReader}
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

final class TokenRepositoryUrl private (val value: String) extends AnyVal with UrlTinyType
object TokenRepositoryUrl
    extends TinyTypeFactory[TokenRepositoryUrl](new TokenRepositoryUrl(_))
    with Url
    with UrlOps[TokenRepositoryUrl] {

  private implicit val configReader: ConfigReader[TokenRepositoryUrl] = urlTinyTypeReader(TokenRepositoryUrl)

  def apply[F[_]: MonadThrow](
      config: Config = ConfigFactory.load
  ): F[TokenRepositoryUrl] =
    find[F, TokenRepositoryUrl]("services.token-repository.url", config)
}
