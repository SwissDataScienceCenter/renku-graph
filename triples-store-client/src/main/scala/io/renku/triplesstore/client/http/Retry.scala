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

package io.renku.triplesstore.client.http

import cats.MonadThrow
import cats.effect._
import cats.kernel.Monoid
import cats.syntax.all._
import fs2.Stream
import io.renku.triplesstore.client.http.Retry.Context
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

final class Retry[F[_]: Logger: Temporal: MonadThrow](interval: FiniteDuration, maxTries: Int) {
  private[this] val logger: Logger[F] = Logger[F]
  private[this] val F = MonadThrow[F]

  def retryWhen[A](filter: Throwable => Boolean)(fa: F[A]): F[A] = {
    val waits = Stream.awakeDelay(interval).void

    val tries =
      (Stream.eval(fa.attempt) ++
        waits
          .zip(Stream.repeatEval(fa.attempt))
          .map(_._2)).zipWithIndex.take(maxTries)

    val result =
      tries
        .flatMap {
          case (Right(v), _) => Stream.emit(Context.success(v))
          case (Left(ex), currentTry) if filter(ex) =>
            Stream
              .eval(logger.info(s"Failing with ${ex.getMessage}, trying again $currentTry/$maxTries"))
              .as(Context.failed[A](ex))

          case (Left(ex), _) =>
            Stream.raiseError(ex)
        }
        .takeThrough(_.valueAbsent)
        .compile
        .foldMonoid

    result.map(_.toEither).flatMap {
      case Right(v)   => v.pure[F]
      case Left(errs) => F.raiseError(Retry.RetryExceeded(interval, maxTries, errs))
    }
  }

  def retryConnectionError[A](fa: F[A]): F[A] =
    retryWhen(ConnectionError.exists)(fa)
}

object Retry {
  final case class RetryExceeded(interval: FiniteDuration, maxTries: Int, errors: List[Throwable])
      extends RuntimeException(s"Fail after trying $maxTries times at $interval interval", errors.headOption.orNull) {
    override def fillInStackTrace() = this
  }

  def apply[F[_]: Logger: Temporal: MonadThrow](interval: FiniteDuration, maxTries: Int): Retry[F] =
    new Retry[F](interval, maxTries)

  private final case class Context[A](value: Option[A], errors: List[Throwable]) {
    def valueAbsent: Boolean                    = value.isEmpty
    def toEither:    Either[List[Throwable], A] = value.toRight(errors)
    private def merge(c: Context[A]): Context[A] =
      Context(value.orElse(c.value), c.errors ::: errors)
  }
  private object Context {
    def success[A](v: A):         Context[A] = Context(v.some, Nil)
    def failed[A](ex: Throwable): Context[A] = Context(None, List(ex))

    implicit def monoid[A]: Monoid[Context[A]] =
      Monoid.instance[Context[A]](Context(None, Nil), _ merge _)
  }
}
