package io.renku.cache

import cats.effect._
import cats.syntax.all._

trait Cache[F[_], A, B] {

  def withCache(f: A => F[Option[B]]): A => F[Option[B]]
}

object Cache {

  def noop[F[_], A, B]: Cache[F, A, B] =
    (f: A => F[Option[B]]) => f

  def memory[F[_] : Sync, A, B](size: Int): F[Cache[F, A, B]] =
    if (size <= 0) noop[F, A, B].pure[F]
    else
      Ref.of[F, Map[A, Option[B]]](Map.empty).map {
        state =>
          (f: A => F[Option[B]]) =>
            a =>
              state.get.map(_.get(a)).flatMap {
                case Some(b) => b.pure[F]
                case None =>
                  f(a).flatTap { r =>
                    state.update { b =>
                      if (b.size >= size) b.drop(b.size - size - 1).updated(a, r)
                      else b.updated(a, r)
                    }
                  }
              }
      }
}
