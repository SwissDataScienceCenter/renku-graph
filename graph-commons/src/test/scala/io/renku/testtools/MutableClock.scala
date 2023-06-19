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

package io.renku.testtools

import cats.Applicative
import cats.effect._

import scala.concurrent.duration._

final class MutableClock[F[_]: Applicative](ts: Ref[F, FiniteDuration]) extends Clock[F] {
  override def applicative: Applicative[F] = Applicative[F]

  override def monotonic: F[FiniteDuration] = ts.get

  override def realTime: F[FiniteDuration] = ts.get

  def set(time: FiniteDuration): F[Unit] = ts.set(time)

  def update(f: FiniteDuration => FiniteDuration): F[Unit] = ts.update(f)

  override def toString: String = s"MutableClock($ts)"
}

object MutableClock {

  def create[F[_]: Sync](initial: FiniteDuration): MutableClock[F] =
    new MutableClock[F](Ref.unsafe(initial))

  def apply(initial: FiniteDuration): MutableClock[IO] = create[IO](initial)

  def zero: MutableClock[IO] = create(Duration.Zero)

  def realTime: IO[MutableClock[IO]] =
    for {
      now <- IO.realTime
      ref <- Ref.of[IO, FiniteDuration](now)
    } yield new MutableClock[IO](ref)

  def monotonic: IO[MutableClock[IO]] =
    for {
      now <- IO.monotonic
      ref <- Ref.of[IO, FiniteDuration](now)
    } yield new MutableClock[IO](ref)
}
