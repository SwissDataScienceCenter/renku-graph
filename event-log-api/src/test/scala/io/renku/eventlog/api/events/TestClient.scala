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

package io.renku.eventlog.api.events

import cats.effect.{Async, Ref, Temporal}
import cats.syntax.all._
import scala.concurrent.duration._

class ExpectingTestClient[F[_]: Async] extends Client[F] {

  private val responses: Ref[F, Map[Any, F[Unit]]] = Ref.unsafe(Map.empty)

  def expect[E](event: E, doReturn: F[Unit])(implicit ev: Dispatcher[F, E]): F[Unit] =
    responses.update(_ + (event -> doReturn))

  override def send[E](event: E)(implicit dispatcher: Dispatcher[F, E]): F[Unit] =
    responses.get >>= {
      _.find(_._1 == event)
        .fold(ifEmpty = new Exception(s"Event '$event' not expected to be send").raiseError[F, Unit])(_._2)
    }
}

class CollectingTestClient[F[_]: Async] extends Client[F] {

  private val events: Ref[F, List[Any]] = Ref.unsafe(Nil)

  def collectedEvents: F[List[Any]] = events.get
  def collectedEvents[E](ofType: Class[E]): F[List[E]] =
    events.get.map(_.filter(_.getClass == ofType).asInstanceOf[List[E]])

  def waitForArrival[E](ofType: Class[E]): F[List[E]] =
    events.get
      .map(_.filter(_.getClass == ofType).asInstanceOf[List[E]])
      .flatMap {
        case Nil => Temporal[F].delayBy(waitForArrival(ofType), 500 millis)
        case l   => l.pure[F]
      }

  def waitForArrival[E](event: E): F[Boolean] =
    events.get
      .map(_.exists(_ == event))
      .flatMap {
        case false => Temporal[F].delayBy(waitForArrival(event), 500 millis)
        case true  => true.pure[F]
      }

  def waitForArrival[E](expected: List[E]): F[Boolean] =
    events.get
      .map(l => (expected diff l).isEmpty)
      .flatMap {
        case true  => true.pure[F]
        case false => Temporal[F].delayBy(waitForArrival(expected), 500 millis)
      }

  override def send[E](event: E)(implicit dispatcher: Dispatcher[F, E]): F[Unit] =
    events.update(_ appended event)
}

object TestClient {
  def expectingMode[F[_]:  Async]: ExpectingTestClient[F]  = new ExpectingTestClient[F]
  def collectingMode[F[_]: Async]: CollectingTestClient[F] = new CollectingTestClient[F]
}
