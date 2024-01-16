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

package io.renku.core.client

import cats.Monad
import cats.data.Nested
import cats.syntax.all._

private object NestedResult {
  def apply[F[_]: Monad]: NestedResult[F] = new NestedResult[F]
}

private class NestedResult[F[_]: Monad] {

  type NestedResult[A] = Nested[F, Result, A]

  implicit lazy val nestedResultMonad: Monad[NestedResult] = new Monad[NestedResult] {

    override def pure[A](a: A): NestedResult[A] =
      Nested {
        Result.success(a).pure[F]
      }

    override def flatMap[A, B](fa: NestedResult[A])(f: A => NestedResult[B]): NestedResult[B] =
      Nested {
        fa.value >>= {
          case Result.Success(a) => f(a).value
          case r: Result.Failure => r.pure[F].widen
        }
      }

    override def map[A, B](fa: NestedResult[A])(f: A => B): NestedResult[B] =
      Nested {
        fa.value.map(_.map(f))
      }

    override def tailRecM[A, B](a: A)(f: A => NestedResult[Either[A, B]]): NestedResult[B] =
      Nested[F, Result, B](
        Monad[F].tailRecM[Result[A], Result[B]](Result.success(a)) {
          case failure: Result.Failure => failure.asInstanceOf[Result[B]].asRight[Result[A]].pure[F]
          case Result.Success(v) =>
            f(v).value.map {
              case failure: Result.Failure => failure.asRight[Result[A]]
              case Result.Success(Left(a))  => Left(Result.success(a))
              case Result.Success(Right(a)) => Right(Result.success(a))
            }
        }
      )
  }

  implicit class NestedResultOps[A](nestedF: NestedResult[A]) {

    def flatMapF[B](f: A => F[Result[B]]): NestedResult[B] =
      nestedF.flatMap(a => Nested(f(a)))

    def subflatMap[B](f: A => Result[B]): NestedResult[B] =
      nestedF.flatMap(a => Nested(f(a).pure[F]))
  }
}
