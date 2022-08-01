/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import eu.timepit.refined.auto._
import io.renku.config.ServiceVersion
import io.renku.events.consumers.subscriptions.{SubscriberUrl, SubscriptionMechanism}
import io.renku.metrics.MetricsRegistry
import io.renku.microservices.MicroserviceUrlFinder
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.Microservice
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.typelevel.log4cats.Logger

object SubscriptionFactory {

  def apply[F[_]: Async: ReProvisioningStatus: Logger: MetricsRegistry: SparqlQueryTimeRecorder](
      config: Config
  ): F[(EventHandler[F], SubscriptionMechanism[F])] = for {
    urlFinder      <- MicroserviceUrlFinder[F](Microservice.ServicePort)
    subscriberUrl  <- urlFinder.findBaseUrl().map(SubscriberUrl(_, "events"))
    serviceVersion <- ServiceVersion.readFromConfig()
    subscriptionMechanism <-
      SubscriptionMechanism[F](
        categoryName,
        PayloadComposer.payloadsComposerFactory[F](subscriberUrl, Microservice.Identifier, serviceVersion)
      )
    handler <- EventHandler(subscriberUrl, Microservice.Identifier, serviceVersion, subscriptionMechanism, config)
  } yield handler -> subscriptionMechanism
}
