/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.docs

import cats.effect.IO
import io.renku.config.ServiceVersion
import io.renku.testtools.IOSpec
import io.swagger.parser.OpenAPIParser
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "GET /spec.json" should {

    "return the OpenAPI specification" in new TestCase {

      val contents = endpoint.`get /docs`.unsafeRunSync().as[String].unsafeRunSync()
      println(contents)

      Option(new OpenAPIParser().readContents(contents, null, null).getOpenAPI).isEmpty shouldBe false
    }
  }

  private trait TestCase {
    val endpoint = new EndpointImpl[IO](ServiceVersion("0.0.0"))
  }
}
