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

package ch.datascience.events.consumers.subscriptions

import cats.MonadError
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.microservices._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.circe.Json

trait SubscriptionPayloadComposer[Interpretation[_]] {
  def prepareSubscriptionPayload(): Interpretation[Json]
}

private class SubscriptionPayloadComposerImpl[Interpretation[_]](
    categoryName:           CategoryName,
    microserviceUrlFinder:  MicroserviceUrlFinder[Interpretation],
    microserviceIdentifier: MicroserviceIdentifier
)(implicit ME:              MonadError[Interpretation, Throwable])
    extends SubscriptionPayloadComposer[Interpretation] {

  import io.circe.syntax._
  import microserviceUrlFinder._

  override def prepareSubscriptionPayload(): Interpretation[Json] =
    findBaseUrl()
      .map(newSubscriberUrl)
      .map(SubscriberBasicInfo(_, SubscriberId(microserviceIdentifier)))
      .map(CategoryAndUrlPayload(categoryName, _).asJson)

  private def newSubscriberUrl(baseUrl: MicroserviceBaseUrl) = SubscriberUrl(baseUrl, "events")
}

object SubscriptionPayloadComposer {

  def categoryAndUrlPayloadsComposerFactory(
      microservicePort:       Int Refined Positive,
      microserviceIdentifier: MicroserviceIdentifier
  ): Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO]] =
    Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO]] { categoryName =>
      for {
        subscriptionUrlFinder <- MicroserviceUrlFinder(microservicePort)
      } yield new SubscriptionPayloadComposerImpl[IO](categoryName, subscriptionUrlFinder, microserviceIdentifier)
    }
}
