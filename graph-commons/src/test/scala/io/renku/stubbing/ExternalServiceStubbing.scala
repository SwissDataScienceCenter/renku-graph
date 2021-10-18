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

package io.renku.stubbing

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
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

  override def beforeEach(): Unit =
    server.resetAll()

  override def afterAll(): Unit = {
    server.stop()
    server.shutdownServer()
  }
}
