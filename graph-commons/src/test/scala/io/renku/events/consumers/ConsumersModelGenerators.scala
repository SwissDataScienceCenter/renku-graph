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

package io.renku.events.consumers

import EventSchedulingResult._
import io.renku.generators.Generators.{exceptions, fixed, nonEmptyStrings}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

object ConsumersModelGenerators {

  implicit lazy val consumerProjects: Gen[Project] = for {
    projectId <- projectIds
    path      <- projectPaths
  } yield Project(projectId, path)

  lazy val badRequests: Gen[EventSchedulingResult.BadRequest] = nonEmptyStrings().toGeneratorOf(BadRequest)
  lazy val serviceUnavailables: Gen[EventSchedulingResult.ServiceUnavailable] =
    nonEmptyStrings().toGeneratorOf(ServiceUnavailable)
  lazy val schedulingErrors: Gen[EventSchedulingResult.SchedulingError] = exceptions.toGeneratorOf(SchedulingError)

  lazy val notHappySchedulingResults: Gen[EventSchedulingResult] = Gen.oneOf(
    fixed(Busy),
    fixed(UnsupportedEventType),
    badRequests,
    serviceUnavailables,
    schedulingErrors
  )

  lazy val eventSchedulingResults: Gen[EventSchedulingResult] = Gen.oneOf(
    fixed(Accepted),
    fixed(Busy),
    fixed(UnsupportedEventType),
    badRequests,
    serviceUnavailables,
    schedulingErrors
  )
}
