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

package ch.datascience.triplesgenerator.events.subscriptions

import cats.MonadError
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.graph.model.events.CategoryName

private[events] trait SubscriptionPayloadComposer[Interpretation[_], Payload <: SubscriptionPayload] {
  def prepareSubscriptionPayload(): Interpretation[Payload]
}

private class SubscriptionPayloadComposerImpl[Interpretation[_]](
    categoryName:          CategoryName,
    subscriptionUrlFinder: SubscriptionUrlFinder[Interpretation]
)(implicit ME:             MonadError[Interpretation, Throwable])
    extends SubscriptionPayloadComposer[Interpretation, CategoryAndUrlPayload] {

  import subscriptionUrlFinder._

  override def prepareSubscriptionPayload(): Interpretation[CategoryAndUrlPayload] =
    findSubscriberUrl() map (CategoryAndUrlPayload(categoryName, _))
}

private object SubscriptionPayloadComposer {

  lazy val categoryAndUrlPayloadsComposerFactory
      : Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO, CategoryAndUrlPayload]] =
    Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO, CategoryAndUrlPayload]] { (categoryName: CategoryName) =>
      for {
        subscriptionUrlFinder <- IOSubscriptionUrlFinder()
      } yield new SubscriptionPayloadComposerImpl[IO](categoryName, subscriptionUrlFinder)
    }
}
