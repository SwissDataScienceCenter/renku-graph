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

package io.renku.triplesgenerator

import cats._
import cats.effect.std.Console
import cats.effect.{Resource, Temporal}
import eu.timepit.refined.auto._
import fs2.io.net.Network
import io.renku.db.{DBConfigProvider, SessionPoolResource}
import io.renku.graph.model.projects
import io.renku.lock.{Lock, LongKey, PostgresLock}
import natchez.Trace

import scala.concurrent.duration.FiniteDuration

sealed trait TgLockDB

object TgLockDB {
  type TsWriteLock[F[_]] = Lock[F, projects.Path]

  type SessionResource[F[_]] = io.renku.db.SessionResource[F, TgLockDB]

  object SessionResource {
    def apply[F[_]](implicit sr: SessionResource[F]): SessionResource[F] = sr
  }

  def createLock[F[_]: MonadThrow: Trace: Network: Console: Temporal, A: LongKey](
      interval: FiniteDuration
  ): Resource[F, Lock[F, A]] =
    Resource
      .eval(new TgLockDbConfigProvider[F].map(SessionPoolResource[F, TgLockDB]))
      .flatMap(identity)
      .flatMap(_.session.map(PostgresLock.exclusive[F, A](_, interval)))
}

class TgLockDbConfigProvider[F[_]: MonadThrow]()
    extends DBConfigProvider[F, TgLockDB](
      namespace = "tg-lock",
      dbName = "tg_lock"
    )
