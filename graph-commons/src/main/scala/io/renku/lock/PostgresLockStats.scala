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

import skunk._
import skunk.codec.all._
import skunk.implicits._

object PostgresLockStats {

  final case class Stats(
      database: String,
      objectId: Long,
      pid:      Int,
      granted:  Boolean
  )

  def getStats[F[_]](session: Session[F]): F[List[Stats]] =
    session.execute[Stats](query)

  //   SELECT db.datname :: varchar, pl.objid :: bigint, pl.pid, pl.granted,pl.waitstart, now() - pl.waitstart
  // ^^ unfortunately, we have postgres 12.x and the pl.waitstart was added in version 14
  // see https://www.postgresql.org/docs/14/view-pg-locks.html
  private def query: Query[Void, Stats] =
    sql"""
       SELECT db.datname :: varchar, pl.objid :: bigint, pl.pid, pl.granted
       FROM pg_locks pl
       INNER JOIN pg_database db ON db.oid = pl.database
       WHERE pl.locktype = 'advisory';
    """
      .query(varchar *: int8 *: int4 *: bool)
      .to[Stats]
}
