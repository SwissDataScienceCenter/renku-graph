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
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

class IOReProvisioning(triplesFinder:         TriplesVersionFinder[IO],
                       triplesRemover:        TriplesRemover[IO],
                       eventsReScheduler:     EventsReScheduler[IO],
                       reProvisioningDelay:   ReProvisioningDelay,
                       executionTimeRecorder: ExecutionTimeRecorder[IO],
                       logger:                Logger[IO],
                       sleepWhenBusy:         FiniteDuration)(implicit timer: Timer[IO])
    extends ReProvisioning[IO](triplesFinder,
                               triplesRemover,
                               eventsReScheduler,
                               reProvisioningDelay,
                               executionTimeRecorder,
                               logger,
                               sleepWhenBusy)
