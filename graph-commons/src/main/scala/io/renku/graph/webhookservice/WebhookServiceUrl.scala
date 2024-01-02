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

package io.renku.graph.webhookservice

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.{find, urlTinyTypeReader}
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

final class WebhookServiceUrl private (val value: String) extends AnyVal with UrlTinyType
object WebhookServiceUrl
    extends TinyTypeFactory[WebhookServiceUrl](new WebhookServiceUrl(_))
    with Url[WebhookServiceUrl]
    with UrlOps[WebhookServiceUrl] {

  private implicit val configReader: ConfigReader[WebhookServiceUrl] = urlTinyTypeReader(WebhookServiceUrl)

  def apply[F[_]: MonadThrow](
      config: Config = ConfigFactory.load
  ): F[WebhookServiceUrl] =
    find[F, WebhookServiceUrl]("services.webhook-service.url", config)

}
