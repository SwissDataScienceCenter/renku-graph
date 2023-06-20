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

import cats.Functor
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.concurrent.duration.FiniteDuration

object PostgresLock {

  def apply[F[_]: Temporal, A: LongKey](session: Session[F], interval: FiniteDuration): Lock[F, A] = {
    def acq(n: Long): F[Boolean] =
      session.unique(tryAcquireSql)(n)

    def rel(n: Long): F[Unit] =
      session.unique(releaseSql)(n).void

    val conv = LongKey[A]

    Lock
      .create(Kleisli(acq), interval)(Kleisli(rel))
      .local(conv.asLong)
  }

  def blocking[F[_]: Functor, A: LongKey](session: Session[F]): Lock[F, A] = {
    def acq(n: Long): F[Unit] =
      session.unique(acquireSql)(n).void

    def rel(n: Long): F[Unit] =
      session.unique(releaseSql)(n).void

    val conv = LongKey[A]

    Lock
      .from(Kleisli(acq))(Kleisli(rel))
      .local(conv.asLong)
  }

  // ???
  implicit val voidDecoder: Decoder[Void] =
    Codec.simple[Void](_ => "null", _ => Right(Void), skunk.data.Type.void)

  private def tryAcquireSql: Query[Long, Boolean] =
    sql"SELECT pg_try_advisory_lock($int8)".query(bool)

  private def acquireSql: Query[Long, Void] =
    sql"SELECT pg_advisory_lock($int8)".query(voidDecoder)

  private def releaseSql: Query[Long, Boolean] =
    sql"SELECT pg_unlock_advisory_lock($int8)".query(bool)
}
