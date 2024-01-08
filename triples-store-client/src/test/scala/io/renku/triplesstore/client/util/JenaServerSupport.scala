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

package io.renku.triplesstore.client.util

import cats.effect.{IO, Resource}
import io.renku.triplesstore.client.http.{ConnectionConfig, FusekiClient}
import org.http4s.BasicCredentials
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait JenaServerSupport {

  def server:            JenaServer
  protected val timeout: Duration = 2.minutes

  def clientResource(implicit L: Logger[IO]): Resource[IO, FusekiClient[IO]] =
    FusekiClient[IO](connectionConfig, timeout)

  lazy val connectionConfig =
    ConnectionConfig(server.conConfig.baseUrl, Some(BasicCredentials("admin", "admin")), retry = None)
}
