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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.entities.Person
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import eu.timepit.refined.auto._

private trait UpdatesCreator {

  def prepareUpdates(kgPerson: Person, mergedPerson: Person): List[SparqlQuery] = List(
    nameDeletion(kgPerson, mergedPerson),
    emailDeletion(kgPerson, mergedPerson),
    affiliationDeletion(kgPerson, mergedPerson),
    gitLabIdDeletion(kgPerson, mergedPerson)
  ).flatten

  private def nameDeletion(kgPerson: Person, mergedPerson: Person) =
    Option.when(kgPerson.name != mergedPerson.name) {
      val resource = kgPerson.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - person name delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:name ?name }
            |WHERE { $resource schema:name ?name }
            |""".stripMargin
      )
    }

  private def emailDeletion(kgPerson: Person, mergedPerson: Person) =
    Option.when(kgPerson.maybeEmail exists (!mergedPerson.maybeEmail.contains(_))) {
      val resource = kgPerson.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - person email delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:email ?email }
            |WHERE  { $resource schema:email ?email }
            |""".stripMargin
      )
    }

  private def affiliationDeletion(kgPerson: Person, mergedPerson: Person) =
    Option.when(kgPerson.maybeAffiliation exists (!mergedPerson.maybeAffiliation.contains(_))) {
      val resource = kgPerson.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - person affiliation delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:affiliation ?affiliation }
            |WHERE  { $resource schema:affiliation ?affiliation }
            |""".stripMargin
      )
    }

  private def gitLabIdDeletion(kgPerson: Person, mergedPerson: Person) =
    Option.when(kgPerson.maybeGitLabId exists (!mergedPerson.maybeGitLabId.contains(_))) {
      val resource = kgPerson.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - gitLabId delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE {
            |  $resource schema:sameAs ?sameAsId.
            |  ?sameAsId a schema:URL.
            |  ?sameAsId schema:identifier ?gitLabId.
            |  ?sameAsId schema:additionalType 'GitLab'.
            |}
            |WHERE  {
            |  $resource schema:sameAs ?sameAsId.
            |  ?sameAsId schema:additionalType 'GitLab';
            |            schema:identifier ?gitLabId.
            |}
            |""".stripMargin
      )
    }
}

private object UpdatesCreator extends UpdatesCreator
