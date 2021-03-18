/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import cats.effect.Bracket
import cats.syntax.all._
import ch.datascience.db.SessionResource
import doobie.implicits._
import doobie.util.fragment.Fragment

package object init {

  def execute[Interpretation[_]](
      sql: Fragment
  )(implicit
      transactor: SessionResource[Interpretation, EventLogDB],
      ME:         Bracket[Interpretation, Throwable]
  ): Interpretation[Unit] = sql.update.run.transact(transactor.resource).void
}
