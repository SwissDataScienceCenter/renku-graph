/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.events

import java.time.LocalDateTime
import java.time.ZoneOffset.ofHours

import io.circe._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class CommittedDateSpec extends WordSpec {

  import CommittedDate.committedDateDecoder

  "apply" should {

    "be able to instantiate from date time with zone json string" in {
      val Right(decoded) = committedDateDecoder.decodeJson(Json.fromString("2012-09-20T09:06:12+03:00"))

      decoded shouldBe CommittedDate(LocalDateTime.of(2012, 9, 20, 9, 6, 12).atOffset(ofHours(3)).toInstant)
    }
  }
}
