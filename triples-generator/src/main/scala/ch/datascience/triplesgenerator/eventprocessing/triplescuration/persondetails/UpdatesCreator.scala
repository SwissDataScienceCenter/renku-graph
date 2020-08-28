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

import cats.MonadError
import cats.data.NonEmptyList
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.tinytypes.TinyType
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.persondetails.PersonDetailsUpdater.Person

import scala.language.higherKinds

private[triplescuration] class UpdatesCreator {

  import eu.timepit.refined.auto._

  def prepareUpdates[Interpretation[_]](
      persons:   Set[Person]
  )(implicit ME: MonadError[Interpretation, Throwable]): List[UpdateFunction[Interpretation]] =
    persons.toList flatMap updates

  private def updates[Interpretation[_]](
      implicit ME: MonadError[Interpretation, Throwable]
  ): Person => List[UpdateFunction[Interpretation]] = {
    case Person(id, names, emails) =>
      List(
        namesUpdate(id, names),
        emailsUpdate(id, emails),
        labelsDelete(id)
      ).flatten
  }

  private def namesUpdate[Interpretation[_]](id: ResourceId, names: NonEmptyList[Name])(
      implicit ME:                               MonadError[Interpretation, Throwable]
  ) = Some {
    val resource = id.showAs[RdfResource]
    UpdateFunction[Interpretation](
      s"Updating Person $resource schema:name",
      SparqlQuery(
        name = "upload - person name update",
        Set(
          "PREFIX schema: <http://schema.org/>",
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
        ),
        s"""|DELETE { $resource schema:name ?name }
            |${INSERT(resource, "schema:name", names.toList).getOrElse("")}
            |WHERE { 
            |  OPTIONAL { $resource schema:name ?maybeName }
            |  BIND (IF(BOUND(?maybeName), ?maybeName, "nonexisting") AS ?name)
            |}
            |""".stripMargin
      )
    )
  }

  private def emailsUpdate[Interpretation[_]](id: ResourceId, emails: Set[Email])(
      implicit ME:                                MonadError[Interpretation, Throwable]
  ) = Some {
    val resource = id.showAs[RdfResource]
    UpdateFunction[Interpretation](
      s"Updating Person $resource schema:email",
      SparqlQuery(
        name = "upload - person email update",
        Set(
          "PREFIX schema: <http://schema.org/>",
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
        ),
        s"""|DELETE { $resource schema:email ?email }
            |${INSERT(resource, "schema:email", emails.toList).getOrElse("")}
            |WHERE  { 
            |  OPTIONAL { $resource schema:email ?maybeEmail }
            |  BIND (IF(BOUND(?maybeEmail), ?maybeEmail, "nonexisting") AS ?email)
            |}
            |""".stripMargin
      )
    )
  }

  private def labelsDelete[Interpretation[_]](id: ResourceId)(implicit ME: MonadError[Interpretation, Throwable]) =
    Some {
      val resource = id.showAs[RdfResource]
      UpdateFunction[Interpretation](
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

  private def INSERT[TT <: TinyType { type V = String }](resource: String,
                                                         property: String,
                                                         values:   List[TT]): Option[String] =
    values match {
      case Nil => None
      case list =>
        val triples = list
          .map(tt => s"\t$resource $property '${sparqlEncode(tt.value)}'")
          .mkString(".\n")
        Some(s"""|INSERT {\n
                 |\t$resource rdf:type schema:Person.
                 |$triples\n
                 |}""".stripMargin)
    }
}
