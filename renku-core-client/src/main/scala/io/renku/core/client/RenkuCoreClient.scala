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
import io.renku.graph.model.projects
import io.renku.http.client.{AccessToken, RestClient}
import org.http4s.Header
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger
import org.typelevel.ci._

trait RenkuCoreClient[F[_]] {
  def getMigrationCheck(projectGitHttpUrl: projects.GitHttpUrl,
                        accessToken:       AccessToken
  ): F[Result[ProjectMigrationCheck]]
}

object RenkuCoreClient {
  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[RenkuCoreClient[F]] =
    for {
      coreCurrentUri <- RenkuCoreUri.Current.loadFromConfig[F](config)
      versionClient = RenkuCoreVersionClient[F](coreCurrentUri, config)
    } yield new RenkuCoreClientImpl[F](coreCurrentUri, versionClient, ClientTools[F])
}

private class RenkuCoreClientImpl[F[_]: Async: Logger](currentUri: RenkuCoreUri.Current,
                                                       versionClient: RenkuCoreVersionClient[F],
                                                       clientTools:   ClientTools[F]
) extends RestClient[F, Nothing](Throttler.noThrottling)
    with RenkuCoreClient[F]
    with Http4sDsl[F]
    with Http4sClientDsl[F] {

  import clientTools._

  println(versionClient)

  override def getMigrationCheck(projectGitHttpUrl: projects.GitHttpUrl,
                                 accessToken:       AccessToken
  ): F[Result[ProjectMigrationCheck]] = {

    val uri = (currentUri.uri / "renku" / "cache.migrations_check")
      .withQueryParam("git_url", projectGitHttpUrl.value)

    send(GET(uri).withHeaders(Header.Raw(ci"gitlab-token", accessToken.value))) {
      case (Ok, _, resp) =>
        toResult[ProjectMigrationCheck](resp)
      case reqInfo =>
        toFailure[ProjectMigrationCheck](s"Migration check for $projectGitHttpUrl failed")(reqInfo)
    }
  }
}
