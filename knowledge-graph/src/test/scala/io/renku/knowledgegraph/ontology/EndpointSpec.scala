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
import cats.syntax.all._
import io.circe.Json
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.relativePaths
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import org.http4s.MediaType.{application, text}
import org.http4s.Method.GET
import org.http4s.Status._
import org.http4s.headers.{Accept, Location, `Content-Type`}
import org.http4s.implicits._
import org.http4s.{Charset, Headers, Request}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Paths

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
        "no Accept"         -> Request[IO](GET, uri"index-en.html"),
        "Accept: text/html" -> Request[IO](GET, uri"index-en.html", headers = Headers(Accept(text.html)))
      )
    } { case (caze, request) =>
      // OK responses tests are in the acceptance tests
      s"respond with OK and Renku ontology in JSON-LD when $caze header given" in new TestCase {

        val generationPath = Paths.get(relativePaths().generateOne)
        (() => htmlGenerator.generationPath).expects().returns(generationPath)
        (() => htmlGenerator.generateHtml).expects().returns(().pure[IO])

        val response = endpoint.`GET /ontology`(request.pathInfo)(request).unsafeRunSync()

        response.status                     shouldBe NotFound
        response.contentType                shouldBe Some(`Content-Type`(text.plain, Charset.`UTF-8`))
        response.as[String].unsafeRunSync() shouldBe "Ontology '/index-en.html' resource cannot be found"
      }
    }

    "respond with SEE_OTHER with 'Location: /index-en.html' if no ontology resource given" in new TestCase {

      (() => htmlGenerator.generateHtml).expects().returns(().pure[IO])

      val request = Request[IO](GET, uri"", headers = Headers(Accept(text.html)))

      val response = endpoint.`GET /ontology`(request.pathInfo)(request).unsafeRunSync()

      response.status                shouldBe SeeOther
      response.headers.get[Location] shouldBe Some(Location(request.uri / "index-en.html"))
      response.headers.get[Accept]   shouldBe Some(Accept(text.html))
    }

    "respond with OK and Renku ontology in JSON-LD when 'Accept: application/ld+json' header given" in new TestCase {

      val ontology = jsonLDEntities.generateOne
      (() => ontologyGenerator.getOntology).expects().returns(ontology)

      val request = Request[IO](headers = Headers(Accept(application.`ld+json`)))

      val response = endpoint.`GET /ontology`(request.pathInfo)(request).unsafeRunSync()

      response.status                   shouldBe Ok
      response.contentType              shouldBe Some(`Content-Type`(application.`ld+json`))
      response.as[Json].unsafeRunSync() shouldBe ontology.toJson
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val ontologyGenerator = mock[OntologyGenerator]
    val htmlGenerator     = mock[HtmlGenerator[IO]]
    val endpoint          = new EndpointImpl[IO](ontologyGenerator, htmlGenerator)
  }
}
