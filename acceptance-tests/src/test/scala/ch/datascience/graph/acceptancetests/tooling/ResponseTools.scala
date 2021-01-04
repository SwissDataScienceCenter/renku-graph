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

package ch.datascience.graph.acceptancetests.tooling

import cats.effect.IO
import io.circe.Json
import org.http4s.circe.jsonOf
import org.http4s.{EntityDecoder, Response}
import org.scalatest.Assertions.fail

object ResponseTools {

  private implicit val jsonEntityDecoder: EntityDecoder[IO, Json] = jsonOf[IO, Json]

  implicit class ResponseOps(response: Response[IO]) {

    lazy val bodyAsJson: Json = response.as[Json].unsafeRunSync()

    def headerLink(rel: String): String =
      response.headers.toList
        .find(_.value contains s"""rel="$rel"""")
        .map { header =>
          val value = header.value
          value.substring(value.lastIndexOf("<") + 1, value.lastIndexOf(">"))
        }
        .getOrElse(fail(s"""No link with the rel="$rel""""))

  }
}
