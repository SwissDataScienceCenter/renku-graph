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
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.control.Throttler
import io.renku.graph.model.versions.SchemaVersion
import io.renku.http.client.RestClient
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

private trait RenkuCoreVersionClient[F[_]] {
  def findCoreUri(schemaVersion: SchemaVersion): F[Result[RenkuCoreUri.Versioned]]
  def getVersions: F[Result[List[SchemaVersion]]]
  def getApiVersion(uri: RenkuCoreUri.ForSchema): F[Result[SchemaApiVersions]]
}

private object RenkuCoreVersionClient {
  def apply[F[_]: Async: Logger](coreCurrentUri: RenkuCoreUri.Current,
                                 config:         Config = ConfigFactory.load
  ): RenkuCoreVersionClient[F] =
    new RenkuCoreVersionClientImpl[F](coreCurrentUri, RenkuCoreUri.ForSchema, config, ClientTools[F])
}

private class RenkuCoreVersionClientImpl[F[_]: Async: Logger](coreUri: RenkuCoreUri.Current,
                                                              coreUriForSchemaLoader: RenkuCoreUri.ForSchemaLoader,
                                                              config:                 Config,
                                                              clientTools:            ClientTools[F]
) extends RestClient[F, Nothing](Throttler.noThrottling)
    with RenkuCoreVersionClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  import clientTools._

  override def findCoreUri(schemaVersion: SchemaVersion): F[Result[RenkuCoreUri.Versioned]] =
    for {
      uriForSchema   <- coreUriForSchemaLoader.loadFromConfig[F](schemaVersion, config)
      apiVersionsRes <- getApiVersion(uriForSchema)
    } yield apiVersionsRes.map(_.max).map(RenkuCoreUri.Versioned(uriForSchema, _))

  override def getVersions: F[Result[List[SchemaVersion]]] = {
    val decoder = Decoder.instance[List[SchemaVersion]] { res =>
      val singleVersionDecoder =
        Decoder.instance(_.downField("data").downField("metadata_version").as[SchemaVersion])

      res.downField("versions").as(decodeList(singleVersionDecoder))
    }

    send(GET(coreUri.uri / "renku" / "versions")) {
      case (Ok, _, resp) => toResult[List[SchemaVersion]](resp)(decoder)
      case reqInfo       => toFailure[List[SchemaVersion]](s"Version info cannot be found")(reqInfo)
    }
  }

  override def getApiVersion(uri: RenkuCoreUri.ForSchema): F[Result[SchemaApiVersions]] =
    send(GET(uri.uri / "renku" / "apiversion")) {
      case (Ok, _, resp) =>
        toResult[SchemaApiVersions](resp)
      case reqInfo @ (NotFound, _, _) =>
        toFailure[SchemaApiVersions](s"Api version info for ${uri.uri} does not exist")(reqInfo)
      case reqInfo =>
        toFailure[SchemaApiVersions](s"Finding api version info for ${uri.uri} failed")(reqInfo)
    }
}
