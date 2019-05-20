/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.MonadError
import ch.datascience.webhookservice.eventprocessing.startcommit.CommitToEventLog
import ch.datascience.webhookservice.pushevents.LatestPushEventFetcher
import io.chrisdavenport.log4cats.Logger

import scala.util.Try

private class TryEventsHistoryLoader(
    latestPushEventFetcher: LatestPushEventFetcher[Try],
    commitToEventLog:       CommitToEventLog[Try],
    logger:                 Logger[Try]
)(implicit ME:              MonadError[Try, Throwable])
    extends EventsHistoryLoader[Try](latestPushEventFetcher, commitToEventLog, logger)
