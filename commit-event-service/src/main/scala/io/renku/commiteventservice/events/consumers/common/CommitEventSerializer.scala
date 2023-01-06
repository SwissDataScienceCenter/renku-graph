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

package io.renku.commiteventservice.events.consumers.common

import io.circe.literal._
import io.circe.{Encoder, Json}

private trait CommitEventSerializer {
  def serialiseToJsonString(commitEvent: CommitEvent): String
}

private object CommitEventSerializer extends CommitEventSerializer {

  override def serialiseToJsonString(commitEvent: CommitEvent): String =
    toJson(commitEvent).noSpaces

  private def toJson(commitEvent: CommitEvent): Json = json"""{
    "id":            ${commitEvent.id.value},
    "message":       ${commitEvent.message.value},
    "committedDate": ${commitEvent.committedDate.toString},
    "author":        ${commitEvent.author},
    "committer":     ${commitEvent.committer}, 
    "parents":       ${commitEvent.parents.map(_.value)},
    "project": {
      "id":          ${commitEvent.project.id.value},
      "path":        ${commitEvent.project.path.value}
    }
  }"""

  private implicit def personEncoder[E <: Person]: Encoder[E] = Encoder.instance[E] {
    case person: Person with Person.WithEmail => json"""{
      "username": ${person.name.value},
      "email"   : ${person.email.value}
    }"""
    case person: Person => json"""{
      "username": ${person.name.value}
    }"""
  }
}
