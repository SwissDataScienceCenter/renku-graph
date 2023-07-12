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

import cats.Applicative
import cats.data.Kleisli
import cats.effect._
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.concurrent.duration._

/** Uses postgres' per session advisory locks. It requires the key to be of type bigint, so
 *  callers must provide a `LongKey` instance to do the conversion.
 */
object PostgresLock {

  /** Obtains an exclusive lock, retrying periodically via non-blocking waits */
  def exclusive_[F[_]: Temporal: Logger, A: LongKey](
      session:  Session[F],
      interval: FiniteDuration = 0.5.seconds
  ): Lock[F, A] =
    createPolling[F, A](session, interval, tryAdvisoryLockSql, advisoryUnlockSql)

  /** Obtains an exclusive lock, retrying periodically via non-blocking waits */
  def exclusive[F[_]: Temporal: Logger, A: LongKey](
      session:  Resource[F, Session[F]],
      interval: FiniteDuration = 0.5.seconds
  ): Lock[F, A] =
    Kleisli(key => session.flatMap(exclusive_[F, A](_, interval).run(key)))

  /** Obtains a shared lock, retrying periodically via non-blocking waits. */
  def shared_[F[_]: Temporal: Logger, A: LongKey](
      session:  Session[F],
      interval: FiniteDuration = 0.5.seconds
  ): Lock[F, A] =
    createPolling(session, interval, tryAdvisoryLockSharedSql, advisoryUnlockSharedSql)

  /** Obtains a shared lock, retrying periodically via non-blocking waits. */
  def shared[F[_]: Temporal: Logger, A: LongKey](
      session:  Resource[F, Session[F]],
      interval: FiniteDuration = 0.5.seconds
  ): Lock[F, A] =
    Kleisli(key => session.flatMap(shared_[F, A](_, interval).run(key)))

  private def createPolling[F[_]: Temporal: Logger, A: LongKey](
      session:   Session[F],
      interval:  FiniteDuration,
      lockSql:   Query[Long, Boolean],
      unlockSql: Query[Long, Boolean]
  ): Lock[F, A] = {
    def acq(n: Long): F[Boolean] =
      session
        .unique(lockSql)(n)
        .attempt
        .flatMap {
          case Right(v) => v.pure[F]
          case Left(ex) => Logger[F].warn(ex)(s"Acquiring postgres advisory lock failed! Retry in $interval.").as(false)
        }
        .flatTap {
          case false =>
            PostgresLockStats
              .recordWaiting(session)(n)
              .attempt
              .flatMap(ignoreError[F](s"Failed to write lock stats record for key=$n"))
          case true =>
            PostgresLockStats
              .removeWaiting(session)(n)
              .attempt
              .flatMap(ignoreError[F](s"Failed to remove lock stats record for key=$n"))
        }

    def rel(n: Long): F[Unit] =
      session.unique(unlockSql)(n).void

    Lock
      .create(Kleisli(acq), interval)(Kleisli(rel))
      .local(LongKey[A].asLong)
  }

  private def ignoreError[F[_]: Logger: Applicative](msg: => String)(eab: Either[Throwable, Unit]): F[Unit] =
    eab match {
      case Right(_) => ().pure[F]
      case Left(ex) => Logger[F].error(ex)(msg)
    }

  // how to avoid that boilerplate???
  implicit val void: Codec[Void] =
    Codec.simple[Void](_ => "null", _ => Right(Void), skunk.data.Type.void)

  private def tryAdvisoryLockSql: Query[Long, Boolean] =
    sql"SELECT pg_try_advisory_lock($int8)".query(bool)

  private def tryAdvisoryLockSharedSql: Query[Long, Boolean] =
    sql"SELECT pg_try_advisory_lock_shared($int8)".query(bool)

  private def advisoryUnlockSql: Query[Long, Boolean] =
    sql"SELECT  pg_advisory_unlock($int8)".query(bool)

  private def advisoryUnlockSharedSql: Query[Long, Boolean] =
    sql"SELECT pg_advisory_unlock_shared($int8)".query(bool)
}
