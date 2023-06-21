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

import skunk.codec.all._
import skunk.implicits._
import skunk._

import java.time.OffsetDateTime

object PostgresLockStats {

  final case class Stats(
      database:  String,
      objectId:  Long,
      pid:       Long,
      granted:   Boolean,
      waitstart: OffsetDateTime
  )

  def getStats[F[_]](session: Session[F]): F[List[Stats]] =
    session.execute[Stats](query)

  private def query: Query[Void, Stats] =
    sql"""
       SELECT db.datname, pl.objid, pl.pid, pl.granted,pl.waitstart
       FROM pg_locks pl
       INNER JOIN pg_database db ON db.oid = pl.database
       WHERE pl.locktype = 'advisory';
    """
      .query(varchar *: int8 *: int8 *: bool *: timestamptz)
      .to[Stats]

}
