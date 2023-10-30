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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.effect.Async
import cats.syntax.all._
import fs2.io.net.Network
import io.renku.events.consumers
import io.renku.events.consumers.subscriptions.SubscriptionMechanism
import io.renku.events.consumers.subscriptions.SubscriptionPayloadComposer.defaultSubscriptionPayloadComposerFactory
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.GitLabClient
import io.renku.triplesgenerator.Microservice
import io.renku.triplesgenerator.TgDB.TsWriteLock
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

object SubscriptionFactory {

  def apply[F[
      _
  ]: Async: Network: Logger: ReProvisioningStatus: GitLabClient: AccessTokenFinder: SparqlQueryTimeRecorder](
      tsWriteLock:         TsWriteLock[F],
      projectSparqlClient: ProjectSparqlClient[F]
  ): F[(consumers.EventHandler[F], SubscriptionMechanism[F])] = for {
    subscriptionMechanism <-
      SubscriptionMechanism(
        categoryName,
        defaultSubscriptionPayloadComposerFactory(Microservice.ServicePort, Microservice.Identifier)
      )
    _       <- ReProvisioningStatus[F].registerForNotification(subscriptionMechanism)
    handler <- EventHandler[F](subscriptionMechanism, tsWriteLock, projectSparqlClient)
  } yield handler -> subscriptionMechanism
}
