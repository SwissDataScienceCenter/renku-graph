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

import cats.MonadError
import cats.implicits._
import ch.datascience.dbeventlog.commands.EventLogMarkAllNew

import scala.language.higherKinds

private class ReProvisioner[Interpretation[_]](
    datasetTruncator:   DatasetTruncator[Interpretation],
    eventLogMarkAllNew: EventLogMarkAllNew[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  import datasetTruncator._
  import eventLogMarkAllNew._

  def startReProvisioning: Interpretation[Unit] =
    for {
      _ <- truncateDataset
      _ <- markAllEventsAsNew
    } yield ()
}
