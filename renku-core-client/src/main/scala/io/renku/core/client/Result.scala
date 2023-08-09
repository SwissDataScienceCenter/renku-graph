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

package io.renku.core.client

import cats.FlatMap
import io.circe.Decoder

sealed trait Result[+A] {
  def toEither: Either[Throwable, A]
}

object Result {
  final case class Success[+A](value: A) extends Result[A] {
    def toEither: Either[Throwable, A] = Right(value)
  }

  sealed trait Failure extends RuntimeException with Result[Nothing] {
    def toEither: Either[Throwable, Nothing] = Left(this)
  }

  object Failure {

    final case class Simple(error: String) extends RuntimeException(error) with Failure
    final case class Detailed(code: Int, userMessage: String)
        extends RuntimeException(s"$userMessage: $code")
        with Failure

    object Detailed {
      implicit val decoder: Decoder[Detailed] =
        Decoder.forProduct2("code", "userMessage")(Detailed.apply)
    }
  }

  def success[A](value: A): Result[A] = Success(value)

  def failure[A](error: String): Result[A] = Failure.Simple(error)

  def failure[A](code: Int, userMessage: String): Result[A] = Failure.Detailed(code, userMessage)

  implicit lazy val flatMapOps: FlatMap[Result] = new FlatMap[Result] {

    override def map[A, B](fa: Result[A])(f: A => B): Result[B] =
      fa match {
        case Success(a) => success(f(a))
        case f: Failure => f
      }

    override def flatMap[A, B](fa: Result[A])(f: A => Result[B]): Result[B] =
      fa match {
        case Success(a) => f(a)
        case f: Failure => f
      }

    override def tailRecM[A, B](a: A)(f: A => Result[Either[A, B]]): Result[B] =
      flatMap(f(a)) {
        case Left(v)  => tailRecM[A, B](v)(f)
        case Right(b) => success(b)
      }
  }
}
