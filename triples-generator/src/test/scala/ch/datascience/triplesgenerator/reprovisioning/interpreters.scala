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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.dbeventlog.commands.EventLogMarkAllNew
import ch.datascience.http.client.BasicAuthCredentials

import scala.util.Try

class IOCompleteReProvisioningEndpoint(
    basicAuthCredentials: BasicAuthCredentials,
    reProvisioner:        ReProvisioner[IO]
)(implicit contextShift:  ContextShift[IO], concurrent: Concurrent[IO])
    extends CompleteReProvisioningEndpoint[IO](basicAuthCredentials, reProvisioner)

private class IOReProvisioner(datasetTruncator: DatasetTruncator[IO], eventLogMarkAllNew: EventLogMarkAllNew[IO])
    extends ReProvisioner[IO](datasetTruncator, eventLogMarkAllNew)

class TryEventLogMarkAllNew(transactor: DbTransactor[Try, EventLogDB])(implicit ME: Bracket[Try, Throwable])
    extends EventLogMarkAllNew[Try](transactor)
