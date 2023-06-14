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

package io.renku.cache

import cats.Monad
import cats.effect._
import cats.syntax.all._
import fs2.Stream
import io.renku.cache.CacheConfig.ClearConfig
import org.typelevel.log4cats.Logger

trait Cache[F[_], A, B] {

  def withCache(f: A => F[Option[B]]): A => F[Option[B]]
}

object Cache {
  def apply[F[_], A, B](f: (A => F[Option[B]]) => A => F[Option[B]]): Cache[F, A, B] =
    (n: A => F[Option[B]]) => f(n)

  def noop[F[_], A, B]: Cache[F, A, B] =
    (f: A => F[Option[B]]) => f

  def memoryAsync[F[_]: Async: Logger, A, B](cacheConfig: CacheConfig, clock: Clock[F]): Resource[F, Cache[F, A, B]] =
    if (cacheConfig.isDisabled) Resource.pure(noop[F, A, B])
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(cacheConfig))

      Resource.eval(refState).flatMap { state =>
        val cache = baseCache[F, A, B](state, clock)
        val clear = cacheClearing(cacheConfig, state, clock)

        Async[F]
          .background(fs2.Stream.repeatEval(clear).compile.drain)
          .as(cache)
      }
    }

  def memoryAsyncF[F[_]: Async: Logger, A, B](cacheConfig: CacheConfig, clock: Clock[F]): F[Cache[F, A, B]] =
    if (cacheConfig.isDisabled) noop[F, A, B].pure[F]
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(cacheConfig))

      refState.flatMap { state =>
        val cache = baseCache[F, A, B](state, clock)
        val clear = cacheClearing(cacheConfig, state, clock)
        Spawn[F].start(clear).as(cache)
      }
    }

  private[cache] def cacheClearing[F[_]: Sync: Temporal: Logger, A, B](
      cacheConfig: CacheConfig,
      state:       Ref[F, CacheState[A, B]],
      clock:       Clock[F]
  ) =
    cacheConfig.clearConfig match {
      case ClearConfig.Periodic(_, interval) =>
        Stream
          .awakeEvery(interval)
          .evalMap(_ => clock.realTime)
          .evalMap(currentTime => state.modify(_.shrink(currentTime)))
          .evalMap(n => if (n > 0) Logger[F].trace(s"Periodic cache clean removed $n entries") else Sync[F].unit)
          .compile
          .drain
    }

  private[cache] def baseCache[F[_]: Monad: Logger, A, B](
      state: Ref[F, CacheState[A, B]],
      clock: Clock[F]
  ): Cache[F, A, B] =
    Cache(f =>
      a =>
        clock.realTime.flatMap(t => state.modify(_.get(a, t))).flatMap {
          case CacheResult.Hit(_, b) =>
            Logger[F].trace(s"Cache hit: $a -> $b").as(b)
          case _ =>
            (Key(a)(clock, Monad[F]), f(a)).flatMapN { (k, r) =>
              state.update(_.put(k, r)).as(r)
            }
        }
    )
}
