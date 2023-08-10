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

package io.renku.core.client

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.control.Throttler
import io.renku.http.client.RestClient
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait RenkuCoreClient[F[_]] {}

object RenkuCoreClient {
  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[RenkuCoreClient[F]] =
    for {
      coreCurrentUri <- RenkuCoreUri.Current.loadFromConfig[F](config)
      versionClient = RenkuCoreVersionClient[F](coreCurrentUri, config)
    } yield new RenkuCoreClientImpl[F](coreCurrentUri, versionClient)
}

private class RenkuCoreClientImpl[F[_]: Async: Logger](coreCurrentUri: RenkuCoreUri.Current,
                                                       versionClient: RenkuCoreVersionClient[F]
) extends RestClient[F, Nothing](Throttler.noThrottling)
    with RenkuCoreClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  println(coreCurrentUri)
  println(versionClient)
}
