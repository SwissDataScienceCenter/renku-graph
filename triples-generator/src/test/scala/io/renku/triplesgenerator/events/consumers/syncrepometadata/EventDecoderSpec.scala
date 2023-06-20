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

package io.renku.triplesgenerator.events.consumers.syncrepometadata

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.syntax._
import io.renku.events.EventRequestContent
import io.renku.generators.Generators.Implicits._
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventDecoderSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with EitherValues {

  "decode" should {

    "extract SyncRepoMetadata with project path only if there's no payload in the request" in {
      val event = syncRepoMetadataWithoutPayloadEvents.generateOne
      EventDecoder.decode(EventRequestContent.NoPayload(event.asJson)).value shouldBe event
    }

    "extract SyncRepoMetadata with payload if there are both path and payload in the request" in {
      syncRepoMetadataWithPayloadEvents[IO]
        .map(_.generateOne)
        .asserting {
          case event @ SyncRepoMetadata(_, Some(payload)) =>
            EventDecoder.decode(EventRequestContent.WithPayload(event.asJson, payload)).value shouldBe event
          case _ =>
            fail("expecting payload")
        }
    }
  }
}
