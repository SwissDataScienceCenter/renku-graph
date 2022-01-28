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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.{NonEmptyParallel, Parallel}
import cats.effect.Async
import cats.syntax.all._
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.subscriptions.SubscriptionPayloadComposer.categoryAndUrlPayloadsComposerFactory
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.triplesgenerator.Microservice
import org.typelevel.log4cats.Logger

object SubscriptionFactory {
  def apply[F[_]: Async: NonEmptyParallel: Parallel: Logger](
      metricsRegistry: MetricsRegistry,
      gitLabThrottler: Throttler[F, GitLab],
      timeRecorder:    SparqlQueryTimeRecorder[F]
  ): F[(EventHandler[F], SubscriptionMechanism[F])] = for {
    subscriptionMechanism <- SubscriptionMechanism(
                               categoryName,
                               categoryAndUrlPayloadsComposerFactory(Microservice.ServicePort, Microservice.Identifier)
                             )
    handler <- EventHandler(metricsRegistry, gitLabThrottler, timeRecorder, subscriptionMechanism)
  } yield handler -> subscriptionMechanism
}
