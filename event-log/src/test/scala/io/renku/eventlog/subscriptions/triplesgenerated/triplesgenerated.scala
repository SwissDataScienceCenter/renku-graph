/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectPaths, schemaVersions}
import io.renku.eventlog.EventContentGenerators._
import org.scalacheck.Gen

package object triplesgenerated {

  private[triplesgenerated] lazy val triplesGeneratedEvents: Gen[TriplesGeneratedEvent] = for {
    eventId       <- compoundEventIds
    projectPath   <- projectPaths
    payload       <- eventPayloads
    schemaVersion <- schemaVersions
  } yield TriplesGeneratedEvent(eventId, projectPath, payload, schemaVersion)
}
