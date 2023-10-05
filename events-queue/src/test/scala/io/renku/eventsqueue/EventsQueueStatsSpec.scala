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

import cats.effect.IO
import cats.syntax.all._
import io.circe.syntax._
import io.renku.events.Generators.categoryNames
import io.renku.eventsqueue.Generators.events
import io.renku.generators.Generators.Implicits._
import io.renku.testtools.CustomAsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class EventsQueueStatsSpec extends AsyncFlatSpec with CustomAsyncIOSpec with EventsQueueDBSpec with should.Matchers {

  it should "return stats about number of events per category" in {

    val category1       = categoryNames.generateOne
    val category1Events = events.generateList().map(_.asJson)
    val category2       = categoryNames.generateOne
    val category2Events = events.generateList().map(_.asJson)

    withDB.surround {
      for {
        _ <- category1Events.traverse_(e => execute(repo.insert(category1, e)))
        _ <- category2Events.traverse_(e => execute(repo.insert(category2, e)))
        res <- execute(stats.countsByCategory).asserting(
                 _ shouldBe Map(category1 -> category1Events.size.toLong, category2 -> category2Events.size.toLong)
               )
      } yield res
    }
  }

  private lazy val repo  = new EventsRepositoryImpl[IO, TestDB]
  private lazy val stats = new EventsQueueStatsImpl[IO, TestDB]
}
