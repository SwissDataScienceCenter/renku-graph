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

import Generators._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.Json
import io.circe.syntax._
import io.renku.db.syntax._
import io.renku.events.CategoryName
import io.renku.events.Generators.categoryNames
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventsEnqueuerSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventsQueueDBSpec
    with should.Matchers
    with AsyncMockFactory {

  it should "encode the payload, " +
    "persist it in the DB and " +
    "send a notification over the Channel" in {

      val category = categoryNames.generateOne
      val event    = events.generateOne
      givenPersisting(category, event.asJson, returning = CommandDef.pure[IO])

      for {
        conditionMet <- assertNotifications(category.asChannelId, notifs => notifs.contains(event.asJson.noSpaces))
        _            <- enqueuer.enqueue(category, event)
        _            <- conditionMet.get.flatten
      } yield ()
    }

  private val dbRepository  = mock[DBRepository[IO]]
  private lazy val enqueuer = new EventsEnqueuerImpl[IO, TestDB](dbRepository)

  private def givenPersisting(category: CategoryName, payload: Json, returning: CommandDef[IO]) =
    (dbRepository.insert _)
      .expects(category, payload)
      .returning(returning)
      .atLeastOnce()
}
