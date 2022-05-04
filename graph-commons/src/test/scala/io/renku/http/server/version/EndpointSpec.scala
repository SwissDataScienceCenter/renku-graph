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

package io.renku.http.server.version

import cats.effect.IO
import io.circe.{Decoder, Json}
import io.renku.config.{ServiceName, ServiceVersion}
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.http4s.EntityDecoder
import org.http4s.MediaType.application
import org.http4s.Status.Ok
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "/GET version" should {

    "return OK with json object containing service name and version" in new TestCase {
      val response = endpoint.`GET /version`.unsafeRunSync()

      response.status                                            shouldBe Ok
      response.contentType                                       shouldBe Some(`Content-Type`(application.json))
      response.as[(ServiceName, ServiceVersion)].unsafeRunSync() shouldBe (serviceName -> serviceVersion)
    }
  }

  private trait TestCase {
    val serviceName    = nonEmptyStrings().generateAs(ServiceName)
    val serviceVersion = serviceVersions.generateOne
    val endpoint       = new EndpointImpl[IO](serviceName, serviceVersion)
  }

  private implicit lazy val entityDecoder: EntityDecoder[IO, (ServiceName, ServiceVersion)] = {
    implicit val decoder: Decoder[(ServiceName, ServiceVersion)] = Decoder.instance { cursor =>
      for {
        serviceName    <- cursor.downField("name").as[String].map(ServiceName(_))
        firstVersion   <- cursor.downField("versions").as[List[Json]].map(_.head)
        serviceVersion <- firstVersion.hcursor.downField("version").as[String].map(ServiceVersion(_))
      } yield serviceName -> serviceVersion
    }

    jsonOf[IO, (ServiceName, ServiceVersion)]
  }
}
