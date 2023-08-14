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
import io.renku.graph.model.versions.SchemaVersion
import io.renku.http.client.RestClient
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait RenkuCoreClient[F[_]] {
  def findCoreUri(schemaVersion: SchemaVersion): F[Result[RenkuCoreUri.Versioned]]
}

object RenkuCoreClient {
  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[RenkuCoreClient[F]] =
    RenkuCoreUri.Current.loadFromConfig[F](config).map { coreCurrentUri =>
      new RenkuCoreClientImpl[F](coreCurrentUri, RenkuCoreUri.ForSchema, LowLevelApis[F](coreCurrentUri), config)
    }
}

private class RenkuCoreClientImpl[F[_]: Async: Logger](currentUri: RenkuCoreUri.Current,
                                                       coreUriForSchemaLoader: RenkuCoreUri.ForSchemaLoader,
                                                       lowLevelApis:           LowLevelApis[F],
                                                       config:                 Config
) extends RestClient[F, Nothing](Throttler.noThrottling)
    with RenkuCoreClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  println(currentUri)

  override def findCoreUri(schemaVersion: SchemaVersion): F[Result[RenkuCoreUri.Versioned]] =
    for {
      uriForSchema   <- coreUriForSchemaLoader.loadFromConfig[F](schemaVersion, config)
      apiVersionsRes <- lowLevelApis.getApiVersion(uriForSchema)
    } yield apiVersionsRes.map(_.max).map(RenkuCoreUri.Versioned(uriForSchema, _))

}
