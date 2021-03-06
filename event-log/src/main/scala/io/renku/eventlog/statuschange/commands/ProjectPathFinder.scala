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

package io.renku.eventlog.statuschange.commands

import cats.data.Kleisli
import cats.effect.Bracket
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import skunk._
import skunk.implicits._

private object ProjectPathFinder {

  import io.renku.eventlog.TypeSerializers._

  def findProjectPath[Interpretation[_]: Bracket[*[_], Throwable]](
      eventId: CompoundEventId
  ): Kleisli[Interpretation, Session[Interpretation], projects.Path] = {

    val query: Query[projects.Id, projects.Path] = sql"""SELECT project_path
                                                        FROM project 
                                                        WHERE project_id = $projectIdEncoder
                                                        """.query(projectPathDecoder)

    Kleisli(_.prepare(query).use(_.unique(eventId.projectId)))
  }

}
