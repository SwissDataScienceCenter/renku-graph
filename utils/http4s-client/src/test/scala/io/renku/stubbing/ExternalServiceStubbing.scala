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

package io.renku.stubbing

import cats.effect.{IO, Resource}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{MappingBuilder, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import org.http4s.Uri
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait ExternalServiceStubbing extends BeforeAndAfterEach with BeforeAndAfterAll {
  this: Suite =>

  protected val maybeFixedPort: Option[Int Refined Positive] = None

  private lazy val wireMockConfig =
    maybeFixedPort
      .map(fixedPort => WireMockConfiguration.wireMockConfig().port(fixedPort.value))
      .getOrElse(WireMockConfiguration.wireMockConfig().dynamicPort())

  private lazy val server = {
    val newServer = new WireMockServer(wireMockConfig)
    newServer.start()
    WireMock.configureFor(newServer.port())
    newServer
  }

  lazy val externalServiceBaseUrl: String = s"http://localhost:${server.port()}"
  lazy val externalServiceBaseUri: Uri    = Uri.unsafeFromString(externalServiceBaseUrl)

  override def beforeEach(): Unit =
    server.resetAll()

  override def afterAll(): Unit = {
    server.stop()
    server.shutdownServer()
  }

  protected def otherWireMockResource =
    Resource.make[IO, WireMockServer] {
      IO {
        val config = WireMockConfiguration.wireMockConfig().dynamicPort()
        val server = new WireMockServer(config)

        server.start()

        server
      }
    }(server =>
      IO {
        server.shutdownServer()
      }
    )

  protected implicit class WireMockOps(server: WireMockServer) {

    lazy val baseUri: Uri = Uri.unsafeFromString(server.baseUrl())

    def stubFor(mappingBuilder: MappingBuilder): Unit =
      new WireMock(server.port()).register {
        WireMock.stubFor(mappingBuilder)
      }
  }
}
