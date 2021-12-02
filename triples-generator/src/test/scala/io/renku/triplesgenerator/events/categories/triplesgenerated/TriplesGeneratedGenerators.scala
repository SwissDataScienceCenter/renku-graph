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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import io.renku.events.consumers.ConsumersModelGenerators._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.EventsGenerators._
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.TransformationStepsCreator.TransformationRecoverableError
import org.scalacheck.Gen

private object TriplesGeneratedGenerators {

  lazy val transformationRecoverableErrors: Gen[TransformationRecoverableError] = for {
    message   <- nonEmptyStrings()
    exception <- exceptions
  } yield TransformationRecoverableError(message, exception)

  implicit val triplesGeneratedEvents: Gen[TriplesGeneratedEvent] = for {
    eventId  <- eventIds
    project  <- projectsGen
    entities <- jsonLDEntities
  } yield TriplesGeneratedEvent(eventId, project, entities)

}
