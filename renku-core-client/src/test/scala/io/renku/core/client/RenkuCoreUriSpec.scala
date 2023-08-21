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

import Generators._
import com.typesafe.config.ConfigFactory
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.GraphModelGenerators.projectSchemaVersions
import io.renku.graph.model.versions.SchemaVersion
import org.http4s.Uri
import org.scalatest.TryValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class RenkuCoreUriSpec extends AnyWordSpec with should.Matchers with TryValues {

  "Latest.loadFromConfig" should {

    "read the 'services.renku-core-latest.url' from the Config" in {

      val url    = httpUrls().generateOne
      val config = configForCurrent(url)

      RenkuCoreUri.Latest.loadFromConfig[Try](config).success.value shouldBe
        RenkuCoreUri.Latest(Uri.unsafeFromString(url))
    }

    "fail if the url does not exist" in {
      RenkuCoreUri.Latest.loadFromConfig[Try](ConfigFactory.empty()).failure.exception.getMessage should include(
        "Key not found: 'services'"
      )
    }

    "fail if the url is invalid" in {

      val illegalUrl = "?ddkf !&&"

      val config = configForCurrent(illegalUrl)

      RenkuCoreUri.Latest.loadFromConfig[Try](config).failure.exception.getMessage should include(
        s"'$illegalUrl' is not a valid 'services.renku-core-latest.url' uri"
      )
    }
  }

  "ForSchema.loadFromConfig" should {

    "read the 'services.renku-core-vXXX.url' from the Config" in {

      val uri    = coreUrisForSchema.generateOne
      val config = configForSchema(uri.schemaVersion, uri.uri.renderString)

      RenkuCoreUri.ForSchema.loadFromConfig[Try](uri.schemaVersion, config).success.value shouldBe uri
    }

    "fail if the url does not exist" in {
      RenkuCoreUri.ForSchema
        .loadFromConfig[Try](projectSchemaVersions.generateOne, ConfigFactory.empty())
        .failure
        .exception
        .getMessage should include("Key not found: 'services'")
    }

    "fail if the url is invalid" in {

      val illegalUrl    = "?ddkf !&&"
      val schemaVersion = projectSchemaVersions.generateOne

      val config = configForSchema(schemaVersion, illegalUrl)

      RenkuCoreUri.ForSchema
        .loadFromConfig[Try](schemaVersion, config)
        .failure
        .exception
        .getMessage should include(s"'$illegalUrl' is not a valid 'services.renku-core-v$schemaVersion.url' uri")
    }
  }

  "ForSchema.uri" should {

    "return the same uri that was used on instantiation" in {

      val uri    = Uri.unsafeFromString(httpUrls().generateOne)
      val schema = projectSchemaVersions.generateOne

      RenkuCoreUri.ForSchema(uri, schema).uri shouldBe uri
    }
  }

  "Versioned.uri" should {

    "return a value that is composed from the baseUri and the apiVersion" in {

      val baseUri    = coreUrisForSchema.generateOne
      val apiVersion = apiVersions.generateOne

      RenkuCoreUri.Versioned(baseUri, apiVersion).uri shouldBe baseUri.uri / apiVersion.value
    }
  }

  private def configForCurrent(url: String) =
    ConfigFactory.parseString(
      s"""services {
            renku-core-latest {
              url = "$url"
            }
          }"""
    )

  private def configForSchema(schema: SchemaVersion, url: String) =
    ConfigFactory.parseString(
      s"""services {
            renku-core-v$schema {
              schemaVersion = "$schema"
              url = "$url"
            }
          }"""
    )
}
