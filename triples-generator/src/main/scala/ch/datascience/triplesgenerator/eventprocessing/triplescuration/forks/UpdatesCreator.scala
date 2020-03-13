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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import eu.timepit.refined.auto._

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl) {

  def wasDerivedFromDelete(resourceId: ResourceId) = List {
    val rdfResource = resourceId.showAs[RdfResource]
    Update(
      s"Deleting Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived delete",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
            |WHERE  { $rdfResource prov:wasDerivedFrom ?parentId }
            |""".stripMargin
      )
    )
  }

  def wasDerivedFromInsert(resourceId: ResourceId, forkPath: Path) = List {
    val rdfResource  = resourceId.showAs[RdfResource]
    val forkResource = ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]
    Update(
      s"Inserting Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived insert",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""INSERT DATA { $rdfResource prov:wasDerivedFrom $forkResource }"""
      )
    )
  }

  def unlinkCreator(resourceId: ResourceId): List[Update] = List {
    val rdfResource = resourceId.showAs[RdfResource]
    Update(
      s"Deleting Project $rdfResource schema:creator",
      SparqlQuery(
        name = "upload - project creator unlink",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:creator ?creatorId }
            |WHERE  { $rdfResource schema:creator ?creatorId }
            |""".stripMargin
      )
    )
  }
  def linkCreator(resourceId: ResourceId, newResourceId: users.ResourceId): List[Update] = List {
    val rdfResource     = resourceId.showAs[RdfResource]
    val creatorResource = newResourceId.showAs[RdfResource]
    Update(
      s"Inserting Project $rdfResource schema:creator",
      SparqlQuery(
        name = "upload - project creator link",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""INSERT DATA { $rdfResource schema:creator $creatorResource }"""
      )
    )
  }

  def creatorInsert(resourceId: ResourceId, maybeCreatorEmail: Option[Email], maybeCreatorName: Option[users.Name]) = {
    val rdfResource     = resourceId.showAs[RdfResource]
    val creatorResource = findCreatorId(maybeCreatorEmail)
    List(
      maybeCreatorEmail orElse maybeCreatorName map { _ =>
        Update(
          s"Inserting Project $rdfResource schema:creator",
          SparqlQuery(
            name = "upload - project creator insert",
            Set("PREFIX schema: <http://schema.org/>"),
            s"""INSERT DATA { $rdfResource schema:creator $creatorResource }"""
          )
        )
      },
      maybeCreatorEmail map { email =>
        Update(
          s"Inserting Creator $creatorResource schema:email",
          SparqlQuery(
            name = "upload - creator email insert",
            Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
            s"""|INSERT DATA {
                |  $creatorResource rdf:type <http://schema.org/Person>;
                |                   schema:email '${sparqlEncode(email.value)}'
                |}""".stripMargin
          )
        )
      },
      maybeCreatorName map { name =>
        Update(
          s"Inserting Creator $creatorResource schema:name",
          SparqlQuery(
            name = "upload - creator name insert",
            Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
            s"""|INSERT DATA {
                |  $creatorResource rdf:type <http://schema.org/Person>;
                |                   schema:name '${sparqlEncode(name.value)}'.
                |}""".stripMargin
          )
        )
      }
    ).flatten
  }

  private lazy val findCreatorId: Option[Email] => String = {
    case None => s"<_:${java.util.UUID.randomUUID()}>"
    case Some(email) =>
      val username     = email.extractUsername.value
      val encodedEmail = email.value.replace(username, sparqlEncode(username))
      s"<mailto:$encodedEmail>"
  }

  def dateCreatedDelete(resourceId: ResourceId) = List {
    val rdfResource = resourceId.showAs[RdfResource]
    Update(
      s"Deleting Project $rdfResource schema:dateCreated",
      SparqlQuery(
        name = "upload - project dateCreated delete",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:dateCreated ?date }
            |WHERE  { $rdfResource schema:dateCreated ?date }
            |""".stripMargin
      )
    )
  }

  def dateCreatedInsert(resourceId: ResourceId, dateCreated: DateCreated) = List {
    val rdfResource = resourceId.showAs[RdfResource]
    Update(
      s"Inserting Project $rdfResource schema:dateCreated",
      SparqlQuery(
        name = "upload - project dateCreated insert",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""INSERT DATA { $rdfResource schema:dateCreated '$dateCreated' }"""
      )
    )
  }
}
