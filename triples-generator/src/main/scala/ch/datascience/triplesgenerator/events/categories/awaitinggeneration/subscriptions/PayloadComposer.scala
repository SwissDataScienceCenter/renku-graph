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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.subscriptions

import cats.MonadError
import cats.syntax.all._
import cats.data.Kleisli
import cats.effect.IO
import ch.datascience.events.consumers.subscriptions.SubscriptionPayloadComposer
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.microservices.{IOMicroserviceUrlFinder, MicroserviceUrlFinder}
import ch.datascience.triplesgenerator.Microservice
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.GenerationProcessesNumber
import io.circe.Json

private[awaitinggeneration] class PayloadComposer[Interpretation[_]: MonadError[*[_], Throwable]](
    categoryName: CategoryName,
    capacity:     GenerationProcessesNumber,
    urlFinder:    MicroserviceUrlFinder[Interpretation]
) extends SubscriptionPayloadComposer[Interpretation] {
  import io.circe.syntax._
  import urlFinder._

  override def prepareSubscriptionPayload(): Interpretation[Json] =
    findSubscriberUrl().map(Payload(categoryName, _, capacity).asJson)
}

private[awaitinggeneration] object PayloadComposer {

  lazy val payloadsComposerFactory: Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO]] =
    Kleisli[IO, CategoryName, SubscriptionPayloadComposer[IO]] { categoryName =>
      for {
        subscriptionUrlFinder <- IOMicroserviceUrlFinder(Microservice.ServicePort)
        capacity              <- GenerationProcessesNumber[IO]()
      } yield new PayloadComposer[IO](categoryName, capacity, subscriptionUrlFinder)
    }
}
