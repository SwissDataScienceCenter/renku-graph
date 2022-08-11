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

package io.renku.knowledgegraph.ontology

import cats.effect.IO
import io.circe.Json
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.http4s.MediaType.{application, text}
import org.http4s.{Headers, Request}
import org.http4s.Status._
import org.http4s.headers.{Accept, `Content-Type`}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec
    extends AnyWordSpec
    with should.Matchers
    with MockFactory
    with IOSpec
    with TableDrivenPropertyChecks {

  "GET /ontology" should {

    forAll {
      Table(
        "case"              -> "request",
        "no Accept"         -> Request[IO](),
        "Accept: text/html" -> Request[IO](headers = Headers(Accept(text.html)))
      )
    } { case (caze, request) =>
      s"respond with OK and Renku ontology in JSON-LD when $caze header given" in new TestCase {

        val ontology = nonEmptyStrings().generateOne
        (() => htmlGenerator.getHtml).expects().returns(ontology)

        val response = endpoint.`GET /ontology`(request).unsafeRunSync()

        response.status                     shouldBe Ok
        response.contentType                shouldBe Some(`Content-Type`(text.html))
        response.as[String].unsafeRunSync() shouldBe ontology
      }
    }

    s"respond with OK and Renku ontology in JSON-LD when 'Accept: application/ld+json' header given" in new TestCase {

      val ontology = jsonLDEntities.generateOne
      (() => ontologyGenerator.getOntology).expects().returns(ontology)

      val request = Request[IO](headers = Headers(Accept(application.`ld+json`)))

      val response = endpoint.`GET /ontology`(request).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.`ld+json`))
      response.as[Json].unsafeRunSync() shouldBe ontology.toJson
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val ontologyGenerator = mock[OntologyGenerator]
    val htmlGenerator     = mock[HtmlGenerator]
    val endpoint          = new EndpointImpl[IO](ontologyGenerator, htmlGenerator)
  }
}
