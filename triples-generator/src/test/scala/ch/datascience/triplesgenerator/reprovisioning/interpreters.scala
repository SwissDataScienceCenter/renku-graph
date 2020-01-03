/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.{EventLogFetch, EventLogReScheduler}
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

class IOReProvisioning(triplesFinder:         TriplesVersionFinder[IO],
                       triplesRemover:        TriplesRemover[IO],
                       eventLogReScheduler:   EventLogReScheduler[IO],
                       eventLogFetch:         EventLogFetch[IO],
                       reProvisioningDelay:   ReProvisioningDelay,
                       executionTimeRecorder: ExecutionTimeRecorder[IO],
                       logger:                Logger[IO],
                       sleepWhenBusy:         FiniteDuration)(implicit timer: Timer[IO])
    extends ReProvisioning[IO](triplesFinder,
                               triplesRemover,
                               eventLogReScheduler,
                               eventLogFetch,
                               reProvisioningDelay,
                               executionTimeRecorder,
                               logger,
                               sleepWhenBusy)

class TryEventLogReScheduler(transactor: DbTransactor[Try, EventLogDB])(implicit ME: Bracket[Try, Throwable])
    extends EventLogReScheduler[Try](transactor)
