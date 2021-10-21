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

package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import skunk.codec.all._
import skunk.implicits.toStringOps
import skunk.{Query, Session}

private trait EventTableCheck {

  def whenEventTableExists[F[_]: MonadCancelThrow](
      eventTableExistsMessage: => Kleisli[F, Session[F], Unit],
      otherwise:               => Kleisli[F, Session[F], Unit]
  ): Kleisli[F, Session[F], Unit] = checkTableExists flatMap {
    case true  => eventTableExistsMessage
    case false => otherwise
  }

  private def checkTableExists[F[_]: MonadCancelThrow]: Kleisli[F, Session[F], Boolean] = {
    val query: Query[skunk.Void, Boolean] = sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event')"
      .query(bool)
    Kleisli[F, Session[F], Boolean] { session =>
      session.unique(query).recover { case _ => false }
    }
  }
}
