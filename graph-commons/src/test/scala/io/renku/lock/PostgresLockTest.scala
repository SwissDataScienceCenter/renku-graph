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

package io.renku.lock

import cats.effect._
import io.renku.db.CommonsPostgresSpec
import org.scalatest.Suite
import org.typelevel.log4cats.Logger
import skunk.Session
import skunk.implicits._

import scala.concurrent.duration._

trait PostgresLockTest extends CommonsPostgresSpec { self: Suite =>

  def makeExclusiveLock(s: Session[IO], interval: FiniteDuration = 100.millis)(implicit L: Logger[IO]) =
    PostgresLock.exclusive_[IO, String](s, interval)

  def exclusiveLock(interval: FiniteDuration = 100.millis)(implicit L: Logger[IO]) =
    PostgresLock.exclusive[IO, String](sessionResource, interval)

  def sharedLock(interval: FiniteDuration = 100.millis)(implicit L: Logger[IO]) =
    PostgresLock.shared[IO, String](sessionResource, interval)

  def resetLockTable(s: Session[IO]) =
    PostgresLockStats.ensureStatsTable[IO](s) *>
      s.execute(sql"DELETE FROM kg_lock_stats".command).void
}
