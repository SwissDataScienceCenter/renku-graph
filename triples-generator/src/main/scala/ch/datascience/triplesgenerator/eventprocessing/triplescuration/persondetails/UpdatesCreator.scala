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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails

import cats.data.NonEmptyList
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.`INSERT DATA`
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person

private[triplescuration] class UpdatesCreator {

  import eu.timepit.refined.auto._

  def prepareUpdates(persons: Set[Person]): List[Update] = persons.toList flatMap updates

  private lazy val updates: Person => List[Update] = {
    case Person(id, names, emails) =>
      List(
        namesDelete(id),
        namesInsert(id, names),
        emailsDelete(id),
        emailsInsert(id, emails),
        labelsDelete(id)
      ).flatten
  }

  private def namesDelete(id: ResourceId) = Some {
    val resource = id.showAs[RdfResource]
    Update(
      s"Deleting Person $resource schema:name",
      SparqlQuery(
        name = "upload - person name delete",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $resource schema:name ?name }
            |WHERE  { $resource schema:name ?name }
            |""".stripMargin
      )
    )
  }
  private def namesInsert(id: ResourceId, names: Set[Name]) =
    if (names.isEmpty) None
    else
      Some {
        val resource = id.showAs[RdfResource]
        Update(
          s"Inserting Person $resource schema:name",
          SparqlQuery(
            name = "upload - person name insert",
            Set("PREFIX schema: <http://schema.org/>"),
            s"""|${`INSERT DATA`(resource, "schema:name", NonEmptyList.fromListUnsafe(names.toList))}
                |""".stripMargin
          )
        )
      }

  private def emailsDelete(id: ResourceId) = Some {
    val resource = id.showAs[RdfResource]
    Update(
      s"Deleting Person $resource schema:email",
      SparqlQuery(
        name = "upload - person email delete",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $resource schema:email ?email }
            |WHERE  { $resource schema:email ?email }
            |""".stripMargin
      )
    )
  }

  private def emailsInsert(id: ResourceId, emails: Set[Email]) =
    if (emails.isEmpty) None
    else
      Some {
        val resource = id.showAs[RdfResource]
        Update(
          s"Inserting Person $resource schema:email",
          SparqlQuery(
            name = "upload - person email insert",
            Set("PREFIX schema: <http://schema.org/>"),
            s"""|${`INSERT DATA`(resource, "schema:email", NonEmptyList.fromListUnsafe(emails.toList))}
                |""".stripMargin
          )
        )
      }

  private def labelsDelete(id: ResourceId) = Some {
    val resource = id.showAs[RdfResource]
    Update(
      s"Deleting Person $resource rdfs:label",
      SparqlQuery(
        name = "upload - person label delete",
        Set("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"),
        s"""|DELETE { $resource rdfs:label ?label }
            |WHERE  { $resource rdfs:label ?label }
            |""".stripMargin
      )
    )
  }
}
