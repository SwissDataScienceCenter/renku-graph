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

package io.renku.triplesgenerator.api.events

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.RenkuTinyTypeGenerators._
import org.scalacheck.Gen

object Generators {

  val projectActivatedEvents: Gen[ProjectActivated] =
    (projectPaths -> timestampsNotInTheFuture.toGeneratorOf(ProjectActivated.DateActivated))
      .mapN(ProjectActivated.apply)

  val projectViewedEvents: Gen[ProjectViewedEvent] =
    (projectPaths -> projectViewedDates()).mapN(ProjectViewedEvent.apply)

  val datasetViewedEvents: Gen[DatasetViewedEvent] =
    (datasetIdentifiers -> datasetViewedDates()).mapN(DatasetViewedEvent.apply)

  val projectViewingDeletions: Gen[ProjectViewingDeletion] =
    projectPaths.map(ProjectViewingDeletion.apply)
}
