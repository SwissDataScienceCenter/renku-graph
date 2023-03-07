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

package io.renku.commiteventservice.events.consumers.globalcommitsync

import cats.NonEmptyParallel
import cats.effect._
import cats.syntax.all._
import io.renku.commiteventservice.Microservice
import io.renku.events.consumers
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.subscriptions.SubscriptionPayloadComposer.defaultSubscriptionPayloadComposerFactory
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.logging.ExecutionTimeRecorder
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

object SubscriptionFactory {

  def apply[F[
      _
  ]: Async: NonEmptyParallel: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: ExecutionTimeRecorder]
      : F[(consumers.EventHandler[F], SubscriptionMechanism[F])] = for {
    subscriptionMechanism <-
      SubscriptionMechanism(
        categoryName,
        defaultSubscriptionPayloadComposerFactory(Microservice.ServicePort, Microservice.Identifier)
      )
    handler <- EventHandler(subscriptionMechanism)
  } yield handler -> subscriptionMechanism
}
