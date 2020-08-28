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

import cats.MonadError
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import eu.timepit.refined.auto._

import scala.language.higherKinds

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl) {

  def recreateWasDerivedFrom[Interpretation[_]](
      resourceId: ResourceId,
      forkPath:   Path
  )(implicit ME:  MonadError[Interpretation, Throwable]) = List {
    val rdfResource  = resourceId.showAs[RdfResource]
    val forkResource = ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]
    UpdateFunction[Interpretation](
      s"Updating Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived update",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
            |INSERT { $rdfResource prov:wasDerivedFrom $forkResource }
            |WHERE  { $rdfResource prov:wasDerivedFrom ?parentId }
            |""".stripMargin
      )
    )
  }

  def deleteWasDerivedFrom[Interpretation[_]](
      resourceId: ResourceId
  )(implicit ME:  MonadError[Interpretation, Throwable]) = List {
    val rdfResource = resourceId.showAs[RdfResource]
    UpdateFunction[Interpretation](
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

  def insertWasDerivedFrom[Interpretation[_]](resourceId: ResourceId, forkPath: Path)(
      implicit ME:                                        MonadError[Interpretation, Throwable]
  ) = List {
    val rdfResource  = resourceId.showAs[RdfResource]
    val forkResource = ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]
    UpdateFunction[Interpretation](
      s"Inserting Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived insert",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""INSERT DATA { $rdfResource prov:wasDerivedFrom $forkResource }"""
      )
    )
  }

  def swapCreator[Interpretation[_]](resourceId: ResourceId, newResourceId: users.ResourceId)(
      implicit ME:                               MonadError[Interpretation, Throwable]
  ): List[UpdateFunction[Interpretation]] = {
    val rdfResource     = resourceId.showAs[RdfResource]
    val creatorResource = newResourceId.showAs[RdfResource]
    List(
      UpdateFunction[Interpretation](
        s"Update Project $rdfResource schema:creator",
        SparqlQuery(
          name = "upload - project creator update",
          Set("PREFIX schema: <http://schema.org/>"),
          s"""|DELETE { $rdfResource schema:creator ?creatorId }
              |INSERT { $rdfResource schema:creator $creatorResource }
              |WHERE  { $rdfResource schema:creator ?creatorId }
              |""".stripMargin
        )
      )
    )
  }

  def addNewCreator[Interpretation[_]](
      resourceId:        ResourceId,
      maybeCreatorEmail: Option[Email],
      maybeCreatorName:  Option[users.Name]
  )(implicit ME:         MonadError[Interpretation, Throwable]): List[UpdateFunction[Interpretation]] = {
    val creatorResource = findCreatorId(maybeCreatorEmail)
    insertOrUpdateCreator[Interpretation](maybeCreatorEmail, maybeCreatorName) ++ swapCreator[Interpretation](
      resourceId,
      creatorResource
    )
  }

  private def insertOrUpdateCreator[Interpretation[_]](
      maybeCreatorEmail: Option[Email],
      maybeCreatorName:  Option[users.Name]
  )(implicit ME:         MonadError[Interpretation, Throwable]): List[UpdateFunction[Interpretation]] = {
    val creatorResource = findCreatorId(maybeCreatorEmail).showAs[RdfResource]
    List(
      maybeCreatorEmail -> maybeCreatorName match {
        case (Some(email), Some(name)) =>
          UpdateFunction[Interpretation](
            s"Updating Creator $creatorResource schema:name",
            SparqlQuery(
              name = "upload - creator update",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|DELETE { $creatorResource schema:name ?name }
                  |INSERT { $creatorResource rdf:type <http://schema.org/Person>;
                  |         schema:email '${sparqlEncode(email.value)}';
                  |         schema:name '${sparqlEncode(name.value)}' .
                  |}
                  |WHERE  { 
                  | OPTIONAL { $creatorResource schema:name ?maybeName} 
                  | BIND (IF(BOUND(?maybeName), ?maybeName, "nonexisting") AS ?name)
                  | }
                  |""".stripMargin
            )
          ).some
        case (Some(email), None) =>
          UpdateFunction[Interpretation](
            s"Inserting Creator $creatorResource schema:email",
            SparqlQuery(
              name = "upload - creator insert email",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|INSERT DATA {
                  |  $creatorResource rdf:type <http://schema.org/Person>;
                  |                   schema:email '${sparqlEncode(email.value)}' .
                  |}""".stripMargin
            )
          ).some
        case (None, Some(name)) =>
          UpdateFunction[Interpretation](
            s"Inserting Creator $creatorResource schema:name",
            SparqlQuery(
              name = "upload - creator insert name",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|INSERT DATA {
                  |  $creatorResource rdf:type <http://schema.org/Person>;
                  |                   schema:name '${sparqlEncode(name.value)}' .
                  |}""".stripMargin
            )
          ).some
        case _ => None
      }
    ).flatten
  }

  private lazy val findCreatorId: Option[Email] => users.ResourceId = {
    case None => users.ResourceId(s"_:${java.util.UUID.randomUUID()}")
    case Some(email) =>
      val username     = email.extractName.value
      val encodedEmail = email.value.replace(username, sparqlEncode(username))
      users.ResourceId(s"mailto:$encodedEmail")
  }

  def recreateDateCreated[Interpretation[_]](resourceId: ResourceId, dateCreated: DateCreated)(
      implicit ME:                                       MonadError[Interpretation, Throwable]
  ): List[UpdateFunction[Interpretation]] = {
    val rdfResource = resourceId.showAs[RdfResource]
    List(
      UpdateFunction[Interpretation](
        s"Deleting Project $rdfResource schema:dateCreated",
        SparqlQuery(
          name = "upload - project dateCreated delete",
          Set("PREFIX schema: <http://schema.org/>"),
          s"""|DELETE { $rdfResource schema:dateCreated ?date }
              |WHERE  { $rdfResource schema:dateCreated ?date }
              |""".stripMargin
        )
      ),
      UpdateFunction[Interpretation](
        s"Inserting Project $rdfResource schema:dateCreated",
        SparqlQuery(
          name = "upload - project dateCreated insert",
          Set("PREFIX schema: <http://schema.org/>", "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"),
          s"""INSERT DATA { $rdfResource schema:dateCreated '$dateCreated'^^xsd:dateTime }"""
        )
      )
    )
  }
}
