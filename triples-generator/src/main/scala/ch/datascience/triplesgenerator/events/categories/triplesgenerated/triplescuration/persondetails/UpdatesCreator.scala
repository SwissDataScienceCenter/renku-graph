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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.MonadError
import cats.syntax.all._
import ch.datascience.graph.model.GitLabApiUrl
import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.users.{Email, GitLabId, Name, ResourceId}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.graph.model.views.SparqlValueEncoder.sparqlEncode
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.tinytypes.TinyType
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.CuratedTriples.CurationUpdatesGroup
import eu.timepit.refined.auto._

private class UpdatesCreator(gitLabApiUrl: GitLabApiUrl) {

  def prepareUpdates[Interpretation[_]](
      persons:   Person
  )(implicit ME: MonadError[Interpretation, Throwable]): CurationUpdatesGroup[Interpretation] =
    CurationUpdatesGroup[Interpretation](
      name = "Persons details updates",
      updates(persons): _*
    )

  private def updates: Person => List[SparqlQuery] = { case Person(id, maybeGitLabId, name, maybeEmail) =>
    List(
      fixLinksForNewPersonWithSameGitLabId(id, maybeGitLabId),
      nameUpsert(id, name),
      maybeEmailUpsert(id, maybeEmail),
      maybeGitLabIdUpsert(id, maybeGitLabId),
      labelsDelete(id),
      deleteOldPersonWithSameGitLabId(id, maybeGitLabId)
    ).flatten
  }

  private def fixLinksForNewPersonWithSameGitLabId(id: ResourceId, maybeGitLabId: Option[GitLabId]) =
    maybeGitLabId.map { gitLabId =>
      val resource = id.showAs[RdfResource]
      SparqlQuery.of(
        name = "upload - person link fixing",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { ?id ?property ?personId }
            |INSERT { ?id ?property $resource }
            |WHERE {
            |  ?personId schema:sameAs ?maybeSameAsId.
            |  ?maybeSameAsId schema:additionalType 'GitLab';
            |                 schema:identifier $gitLabId.
            |  ?id ?property ?personId
            |}
            |""".stripMargin
      )
    }

  private def nameUpsert(id: ResourceId, name: Name) = Some {
    val resource = id.showAs[RdfResource]
    SparqlQuery.of(
      name = "upload - person name upsert",
      Prefixes.of(schema -> "schema", rdf -> "rdf"),
      s"""|DELETE { $resource schema:name ?name }
          |${INSERT(resource, "schema:name", name)}
          |WHERE { 
          |  OPTIONAL { $resource schema:name ?maybeName }
          |  BIND (IF(BOUND(?maybeName), ?maybeName, "nonexisting") AS ?name)
          |}
          |""".stripMargin
    )
  }

  private def maybeEmailUpsert(id: ResourceId, maybeEmail: Option[Email]) = {
    val resource = id.showAs[RdfResource]

    maybeEmail match {
      case Some(email) =>
        SparqlQuery.of(
          name = "upload - person email upsert",
          Prefixes.of(schema -> "schema", rdf -> "rdf"),
          s"""|DELETE { $resource schema:email ?email }
              |${INSERT(resource, "schema:email", email)}
              |WHERE  { 
              |  OPTIONAL { $resource schema:email ?maybeEmail }
              |  BIND (IF(BOUND(?maybeEmail), ?maybeEmail, "nonexisting") AS ?email)
              |}
              |""".stripMargin
        )
      case None =>
        SparqlQuery.of(
          name = "upload - person email delete",
          Prefixes.of(schema -> "schema", rdf -> "rdf"),
          s"""|DELETE { $resource schema:email ?email }
              |WHERE  { 
              |  OPTIONAL { $resource schema:email ?maybeEmail }
              |  BIND (IF(BOUND(?maybeEmail), ?maybeEmail, "nonexisting") AS ?email)
              |}
              |""".stripMargin
        )
    }
  }.some

  private def maybeGitLabIdUpsert(id: ResourceId, maybeGitLabId: Option[GitLabId]) = {
    val resource = id.showAs[RdfResource]

    maybeGitLabId match {
      case Some(gitLabId) =>
        SparqlQuery.of(
          name = "upload - gitlab id upsert",
          Prefixes.of(schema -> "schema", rdf -> "rdf"),
          s"""|DELETE { 
              |  $resource schema:sameAs ?sameAsId.
              |  ?sameAsId rdf:type schema:URL.
              |  ?sameAsId schema:identifier ?gitLabId.
              |  ?sameAsId schema:additionalType 'GitLab'.
              |}
              |${INSERT(resource, gitLabId)}
              |WHERE  { 
              |  OPTIONAL { 
              |    $resource schema:sameAs ?maybeSameAsId.
              |    ?maybeSameAsId schema:additionalType 'GitLab';
              |                   schema:identifier ?gitLabId.
              |  }
              |  BIND (IF(BOUND(?maybeSameAsId), ?maybeSameAsId, "nonexisting") AS ?sameAsId)
              |}
              |""".stripMargin
        )
      case None =>
        SparqlQuery.of(
          name = "upload - gitlab id delete",
          Prefixes.of(schema -> "schema", rdf -> "rdf"),
          s"""|DELETE { 
              |  $resource schema:sameAs ?sameAsId.
              |  ?sameAsId rdf:type schema:URL.
              |  ?sameAsId schema:identifier ?gitLabId.
              |  ?sameAsId schema:additionalType 'GitLab'.
              |}
              |WHERE  { 
              |  OPTIONAL { 
              |    $resource schema:sameAs ?maybeSameAsId.
              |    ?maybeSameAsId schema:additionalType 'GitLab';
              |                   schema:identifier ?gitLabId.
              |  }
              |  BIND (IF(BOUND(?maybeSameAsId), ?maybeSameAsId, "nonexisting") AS ?sameAsId)
              |}
              |""".stripMargin
        )
    }
  }.some

  private def labelsDelete(id: ResourceId) = Some {
    val resource = id.showAs[RdfResource]
    SparqlQuery.of(
      name = "upload - person label delete",
      Prefixes.of(rdfs -> "rdfs"),
      s"""|DELETE { $resource rdfs:label ?label }
          |WHERE  { $resource rdfs:label ?label }
          |""".stripMargin
    )
  }

  private def deleteOldPersonWithSameGitLabId(id: ResourceId, maybeGitLabId: Option[GitLabId]) =
    maybeGitLabId.map { gitLabId =>
      val resource = id.showAs[RdfResource]
      SparqlQuery.of(
        name = "upload - same gitLabId person delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { ?personId ?property ?value }
            |WHERE {
            |  ?personId schema:sameAs ?maybeSameAsId.
            |  ?maybeSameAsId schema:additionalType 'GitLab';
            |                 schema:identifier $gitLabId.
            |  FILTER (?personId != $resource)
            |  ?personId ?property ?value
            |}
            |""".stripMargin
      )
    }

  private def INSERT[TT <: TinyType { type V = String }](resource: String, property: String, value: TT): String =
    s"""|INSERT {
        |\t$resource rdf:type schema:Person.
        |\t$resource $property '${sparqlEncode(value.value)}'
        |}""".stripMargin

  private def INSERT(resource: String, gitLabId: GitLabId): String = {
    val sameAsId = (gitLabApiUrl / "users" / gitLabId).showAs[RdfResource]
    s"""|INSERT {
        |\t$resource schema:sameAs $sameAsId.
        |\t$sameAsId rdf:type schema:URL.
        |\t$sameAsId schema:identifier $gitLabId.
        |\t$sameAsId schema:additionalType 'GitLab'.
        |}""".stripMargin
  }

}
