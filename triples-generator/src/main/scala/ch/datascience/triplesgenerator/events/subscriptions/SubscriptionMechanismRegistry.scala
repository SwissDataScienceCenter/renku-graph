/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events
package subscriptions

import cats.{MonadError, Parallel}
import cats.syntax.all._
import cats.effect.{ContextShift, Effect, IO, Timer}
import ch.datascience.graph.model.events.CategoryName
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait SubscriptionMechanismRegistry[Interpretation[_]] {
  def apply(categoryName: CategoryName): Interpretation[SubscriptionMechanism[Interpretation]]
  def renewAllSubscriptions(): Interpretation[Unit]
  def run():                   Interpretation[Unit]
}

class SubscriptionMechanismRegistryImpl[Interpretation[_]: Effect](
    subscriptionsMechanisms: SubscriptionMechanism[Interpretation]*
)(implicit ME:               MonadError[Interpretation, Throwable], parallel: Parallel[Interpretation])
    extends SubscriptionMechanismRegistry[Interpretation] {

  override def apply(categoryName: CategoryName): Interpretation[SubscriptionMechanism[Interpretation]] =
    subscriptionsMechanisms
      .find(_.categoryName == categoryName)
      .map(ME.pure)
      .getOrElse(ME.raiseError(new IllegalStateException(s"No SubscriptionMechanism for $categoryName")))

  override def run(): Interpretation[Unit] = subscriptionsMechanisms.toList.map(_.run()).parSequence.void

  override def renewAllSubscriptions(): Interpretation[Unit] =
    subscriptionsMechanisms.toList.map(_.renewSubscription()).parSequence.void
}

object SubscriptionMechanismRegistry {

  def apply(logger:     Logger[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[SubscriptionMechanismRegistry[IO]] = for {
    awaitingGenerationSubscription <-
      SubscriptionMechanism(categories.awaitinggeneration.EventHandler.categoryName, logger)
  } yield new SubscriptionMechanismRegistryImpl[IO](awaitingGenerationSubscription)
}
