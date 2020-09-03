/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.graph.acceptancetests.data.currentCliVersion
import ch.datascience.graph.model.CliVersion
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.circe.literal._

import scala.annotation.tailrec

object RdfStoreStub {
  import com.github.tomakehurst.wiremock.WireMockServer

  private lazy val fusekiStub = new WireMockServer(wireMockConfig().port(3030))

  def start(): Unit = {
    fusekiStub.start()
    WireMock.configureFor(fusekiStub.port())
    givenTriplesUpToDateCheckReturning(currentCliVersion)
  }

  def shutdown(): Unit = {
    if (fusekiStub.isRunning) fusekiStub.shutdown()

    @tailrec
    def waitUntilDown(): Unit =
      if (fusekiStub.isRunning) {
        Thread.sleep(100)
        waitUntilDown()
      }

    waitUntilDown()
  }

  def givenRenkuDatasetExists(): Unit = {
    stubFor {
      get("/$/datasets/renku")
        .willReturn(ok())
    }
    ()
  }

  private def givenTriplesUpToDateCheckReturning(version: CliVersion): Unit = {
    stubFor {
      post("/renku/sparql")
        .willReturn(okJson(json"""{
          "results": {
            "bindings": [{
              "version": { "value": ${version.toString} }
            }]
          }
        }""".noSpaces))
    }
    ()
  }
}
