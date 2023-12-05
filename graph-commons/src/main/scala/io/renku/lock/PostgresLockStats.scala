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

import cats._
import cats.effect._
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import skunk._
import skunk.codec.all._
import skunk.implicits._

import java.time.{Duration, OffsetDateTime}

object PostgresLockStats {

  final case class Waiting(
      objectId:     Long,
      pid:          Long,
      createdAt:    OffsetDateTime,
      waitDuration: Duration
  )

  final case class Stats(
      currentLocks: Long,
      waiting:      List[Waiting]
  )

  def getStats[F[_]: Monad](dbName: DBConfig.DbName, session: Session[F]): F[Stats] =
    for {
      n <- session.unique(countCurrentLocks)(dbName.value)
      w <- session.execute(queryWaiting)
    } yield Stats(n, w)

  def ensureStatsTable[F[_]: Applicative](session: Session[F]): F[Unit] =
    session.execute(createTable).void

  def recordWaiting[F[_]: MonadCancelThrow](session: Session[F])(key: Long): F[Unit] =
    session.execute(upsertWaiting)(key).void

  def removeWaiting[F[_]: MonadCancelThrow](session: Session[F])(key: Long): F[Unit] =
    session.execute(deleteWaiting)(key).void

  private val countCurrentLocks: Query[String, Long] =
    sql"""
       SELECT COUNT(objid)
       FROM pg_locks l
       JOIN pg_database db ON db.oid = l.database AND db.datname = $text
       WHERE locktype = 'advisory' and granted = true;
    """.query(int8)

  private val queryWaiting: Query[Void, Waiting] =
    sql"""
          SELECT obj_id, sess_id, created_at, now() - created_at
          FROM kg_lock_stats
         """
      .query(int8 *: int8 *: timestamptz *: interval)
      .to[Waiting]

  private val createTable: Command[Void] =
    sql"""
         CREATE TABLE IF NOT EXISTS "kg_lock_stats"(
           obj_id bigint not null,
           sess_id bigint not null,
           created_at timestamptz not null,
           primary key (obj_id, sess_id)
         )""".command

  private val upsertWaiting: Command[Long] =
    sql"""
         INSERT
         INTO kg_lock_stats (obj_id, sess_id, created_at)
         VALUES ($int8, pg_backend_pid(), now())
         ON CONFLICT (obj_id, sess_id) DO NOTHING
         """.command

  private val deleteWaiting: Command[Long] =
    sql"""DELETE FROM kg_lock_stats WHERE obj_id = $int8 AND sess_id = pg_backend_pid()""".command
}
