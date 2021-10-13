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

package io.renku.eventlog

import cats.effect.IO
import ch.datascience.compression.Zip
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.ZippedEventPayload
import io.renku.jsonld.generators.JsonLDGenerators.jsonLDEntities
import org.scalacheck.Gen

object EventContentGenerators {

  implicit val eventDates:     Gen[EventDate]     = timestampsNotInTheFuture map EventDate.apply
  implicit val createdDates:   Gen[CreatedDate]   = timestampsNotInTheFuture map CreatedDate.apply
  implicit val executionDates: Gen[ExecutionDate] = timestamps map ExecutionDate.apply

  implicit val eventMessages: Gen[EventMessage] = nonEmptyStrings() map EventMessage.apply

  implicit val zippedEventPayloads: Gen[ZippedEventPayload] = for {
    content <- jsonLDEntities
    zipped = Zip.zip[IO](content.toJson.noSpaces).unsafeRunSync()
  } yield ZippedEventPayload(zipped)

}
