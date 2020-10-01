/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import cats.data.NonEmptyList
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.CypherQuery
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person
import eu.timepit.refined.auto._

private[triplescuration] class Neo4jUpdatesCreator {

  def prepareUpdates[Interpretation[_]](
      persons:   Person
  )(implicit ME: MonadError[Interpretation, Throwable]): CurationUpdatesGroup[Interpretation, CypherQuery] =
    CurationUpdatesGroup[Interpretation, CypherQuery](
      name = "Persons details updates",
      updates(persons): _*
    )

  private def updates: Person => List[CypherQuery] = { case Person(id, names, emails) =>
    List(
      namesUpdate(id, names),
      emailsUpdate(id, emails),
      labelsDelete(id)
    ).flatten
  }

  private def namesUpdate(id: ResourceId, names: NonEmptyList[Name]) = Some {
    CypherQuery(
      name = "upload - person name update",
      s"""MATCH (n: sch__Person { uri: '$id' })
         |SET n.sch_name = toString(${names.head})
         |RETURN n""".stripMargin
    )
  }

  private def emailsUpdate(id: ResourceId, emails: Set[Email]) = Some {
    CypherQuery(
      name = "upload - person email update",
      s"""MATCH (n: sch__Person { uri: '$id' })
         |SET n.sch_email = toString(${emails.head})
         |RETURN n""".stripMargin
    )
  }

  private def labelsDelete(id: ResourceId) = Some {
    CypherQuery(
      name = "upload - person label delete",
      s"""MATCH (n: sch__Person { uri: '$id' })
         |REMOVE n.rdfs_label
         |RETURN n""".stripMargin
    )
  }
}
