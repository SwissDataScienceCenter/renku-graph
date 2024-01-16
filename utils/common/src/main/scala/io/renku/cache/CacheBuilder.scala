/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect._
import cats.syntax.all._
import cats.{Applicative, Monad}
import fs2.Stream
import io.renku.cache.CacheConfig.ClearConfig

final class CacheBuilder[F[_], A, B](
    clock:   Clock[F],
    config:  CacheConfig,
    handler: List[F[CacheEventHandler[F]]]
) {

  def withConfig(cc: CacheConfig): CacheBuilder[F, A, B] =
    new CacheBuilder[F, A, B](clock, cc, handler)

  def withClock(clock: Clock[F]): CacheBuilder[F, A, B] =
    new CacheBuilder[F, A, B](clock, config, handler)

  def withEventHandler(eh: F[CacheEventHandler[F]])(implicit F: Applicative[F]): CacheBuilder[F, A, B] =
    new CacheBuilder[F, A, B](clock, config, eh :: handler)

  def withEventHandler(eh: CacheEventHandler[F])(implicit F: Applicative[F]): CacheBuilder[F, A, B] =
    withEventHandler(eh.pure[F])

  def withConfigChanged(f: CacheConfig => CacheConfig): CacheBuilder[F, A, B] =
    withConfig(f(config))

  private def createEventHandler(implicit F: Monad[F]): F[CacheEventHandler[F]] =
    handler.sequence.map(CacheEventHandler.combine[F])

  def resource(implicit F: Async[F]): Resource[F, Cache[F, A, B]] =
    if (config.isDisabled) Resource.pure(Cache.noop[F, A, B])
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(config))

      (Resource.eval(refState), Resource.eval(createEventHandler)).flatMapN { (state, handle) =>
        val cache = CacheBuilder.baseCache[F, A, B](state, clock, handle)
        val clear = CacheBuilder.cacheClearing(config, state, clock, handle)

        Async[F]
          .background(fs2.Stream.repeatEval(clear).compile.drain)
          .as(cache)
      }
    }

  def build(implicit F: Async[F]): F[Cache[F, A, B]] =
    if (config.isDisabled) Cache.noop[F, A, B].pure[F]
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(config))

      (refState, createEventHandler).flatMapN { (state, handle) =>
        val cache = CacheBuilder.baseCache[F, A, B](state, clock, handle)
        val clear = CacheBuilder.cacheClearing(config, state, clock, handle)
        Spawn[F].start(clear).as(cache)
      }
    }
}

object CacheBuilder {

  def default[F[_]: Async, A, B]: CacheBuilder[F, A, B] =
    new CacheBuilder[F, A, B](Clock[F], CacheConfig.default, Nil)

  private def cacheClearing[F[_]: Monad: Temporal, A, B](
      cacheConfig: CacheConfig,
      state:       Ref[F, CacheState[A, B]],
      clock:       Clock[F],
      handler:     CacheEventHandler[F]
  ) =
    cacheConfig.clearConfig match {
      case ClearConfig.Periodic(_, interval) =>
        Stream
          .awakeEvery(interval)
          .evalMap(_ => clock.realTime)
          .evalMap(currentTime => state.modify(_.shrink(currentTime)))
          .evalMap(dropped => state.get.map(s => CacheEvent.CacheClear(dropped, s.size)))
          .evalTap(handler.apply)
          .compile
          .drain
    }

  private def baseCache[F[_]: Monad, A, B](
      state:  Ref[F, CacheState[A, B]],
      clock:  Clock[F],
      handle: CacheEventHandler[F]
  ): Cache[F, A, B] =
    Cache(f =>
      a =>
        clock.realTime.flatMap(t => state.modify(_.get(a, t))).flatMap {
          case hit @ CacheResult.Hit(_, b) =>
            state.get
              .map(s => CacheEvent.CacheResponse(hit, s.size))
              .flatMap(handle.apply)
              .as(b)
          case r =>
            val event = state.get.map(s => CacheEvent.CacheResponse(r, s.size))
            (Key(a)(clock, Monad[F]), f(a)).flatMapN { (k, r) =>
              (state.update(_.put(k, r)) *> event.flatMap(handle.apply)).as(r)
            }
        }
    )
}
