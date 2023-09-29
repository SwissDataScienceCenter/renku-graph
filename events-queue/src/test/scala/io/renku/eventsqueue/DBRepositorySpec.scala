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

package io.renku.eventsqueue

import Generators.events
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.syntax._
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class DBRepositorySpec extends AsyncWordSpec with AsyncIOSpec with EventsQueueDBSpec with should.Matchers {

  "insert/eventsStream" should {

    "insert a new row in status 'NEW' into the enqueued_event so the eventsStream can fetch it" in {

      val category = categoryNames.generateOne
      val event    = events.generateOne

      execute(repo.insert(category, event.asJson)).assertNoException >>
        execute(repo.eventsStream(category))
          .flatMap(_.compile.toList)
          .asserting(_.map(_.payload) shouldBe List(event.asJson.noSpaces))
    }
  }

  private lazy val repo = new DBRepositoryImpl[IO, TestDB]
}
