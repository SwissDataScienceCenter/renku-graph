/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.persondetails

import cats.effect.Async
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.Person
import io.renku.graph.model.persons
import io.renku.graph.model.persons.{Email, GitLabId, ResourceId}
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGPersonFinder[F[_]] {
  def find(person: Person): F[Option[Person]]
}

private object KGPersonFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGPersonFinder[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
  } yield new KGPersonFinderImpl(rdfStoreConfig)
}

private class KGPersonFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig)
    extends RdfStoreClientImpl(rdfStoreConfig)
    with KGPersonFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  override def find(person: Person): F[Option[Person]] =
    queryExpecting[Option[Person]](using = findByResourceId(person.resourceId))(recordsDecoder(person))

  private def findByResourceId(resourceId: ResourceId) = SparqlQuery.of(
    name = "transformation - find person by resourceId",
    Prefixes of schema -> "schema",
    s"""|SELECT DISTINCT ?resourceId ?name ?maybeEmail ?maybeGitLabId ?maybeOrcidId ?maybeAffiliation
        |WHERE {
        |  BIND (${resourceId.showAs[RdfResource]} AS ?resourceId)
        |  ?resourceId a schema:Person;
        |              schema:name ?name.
        |  OPTIONAL { ?resourceId schema:email ?maybeEmail }
        |  OPTIONAL {
        |    ?resourceId schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType  'GitLab';
        |              schema:identifier      ?maybeGitLabId.
        |  }
        |  OPTIONAL {
        |    ?resourceId schema:sameAs ?maybeOrcidId.
        |    ?maybeOrcidId schema:additionalType  'Orcid'.
        |  }
        |  OPTIONAL { ?resourceId schema:affiliation ?maybeAffiliation }
        |}
        |LIMIT 1
        |""".stripMargin
  )

  private def recordsDecoder(person: Person): Decoder[Option[Person]] = ResultsDecoder[Option, Person] {
    implicit cursor =>
      import Decoder._
      import io.renku.tinytypes.json.TinyTypeDecoders._

      for {
        resourceId       <- extract[persons.ResourceId]("resourceId")
        name             <- extract[persons.Name]("name")
        maybeEmail       <- extract[Option[persons.Email]]("maybeEmail")
        maybeGitLabId    <- extract[Option[persons.GitLabId]]("maybeGitLabId")
        maybeOrcidId     <- extract[Option[persons.OrcidId]]("maybeOrcidId")
        maybeAffiliation <- extract[Option[persons.Affiliation]]("maybeAffiliation")
        person <- Person
                    .from(resourceId, name, maybeEmail, maybeGitLabId, maybeOrcidId, maybeAffiliation)
                    .toEither
                    .leftMap(errs => DecodingFailure(errs.nonEmptyIntercalate("; "), Nil))
      } yield person
  }(toOption(onMultiple = s"Multiple Person entities found for ${person.resourceId}"))
}
