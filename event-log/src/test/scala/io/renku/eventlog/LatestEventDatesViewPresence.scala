/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog

import cats.effect.IO
import ch.datascience.interpreters.TestLogger
import doobie.implicits._
import io.renku.eventlog.init.LatestEventDatesViewCreator
import org.scalatest.{BeforeAndAfterAll, Suite}

trait LatestEventDatesViewPresence extends BeforeAndAfterAll {
  self: Suite with InMemoryEventLogDbSpec =>

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    LatestEventDatesViewCreator[IO](transactor, TestLogger()).run().unsafeRunSync()
  }

  protected def refreshView(): Unit = verifyTrue {
    sql""" REFRESH MATERIALIZED VIEW CONCURRENTLY project_latest_event_date"""
  }
}
