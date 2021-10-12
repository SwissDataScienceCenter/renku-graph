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

package ch.datascience.events

import ch.datascience.events
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{jsons, nonEmptyStrings}
import org.scalacheck.Gen

object Generators {
  implicit val eventRequestContents: Gen[EventRequestContent] = for {
    event        <- jsons
    maybePayload <- nonEmptyStrings().toGeneratorOfOptions
  } yield maybePayload match {
    case Some(payload) => events.EventRequestContent.WithPayload(event, payload)
    case None          => events.EventRequestContent.NoPayload(event)
  }

  implicit val eventRequestContentNoPayloads = jsons.map(events.EventRequestContent.NoPayload)
}
