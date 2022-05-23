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

package io.renku.rdfstore

import cats.MonadThrow
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.http.rest.paging.PagingRequest
import io.renku.jsonld.Schema
import io.renku.rdfstore.SparqlQuery.Prefix
import io.renku.tinytypes.StringTinyType

final case class SparqlQuery(name:               String Refined NonEmpty,
                             prefixes:           Set[Prefix],
                             body:               String,
                             maybePagingRequest: Option[PagingRequest]
) {
  override lazy val toString: String =
    s"""|${prefixes.mkString("", "\n", "")}
        |$body
        |$pagingRequest""".stripMargin.trim

  private lazy val pagingRequest =
    maybePagingRequest
      .map { pagingRequest =>
        import pagingRequest._
        s"""|LIMIT $perPage
            |OFFSET ${(page.value - 1) * perPage.value}""".stripMargin
      }
      .getOrElse("")
}

object SparqlQuery {

  import io.renku.tinytypes.TinyTypeFactory
  import io.renku.tinytypes.constraints.NonBlank

  def of(
      name:     String Refined NonEmpty,
      prefixes: Set[Prefix],
      body:     String
  ): SparqlQuery = SparqlQuery(name, prefixes map (p => Prefix(p.value)), body, maybePagingRequest = None)

  def apply(
      name:     String Refined NonEmpty,
      prefixes: Set[String Refined NonEmpty],
      body:     String
  ): SparqlQuery = SparqlQuery(name, prefixes.map(p => Prefix(p.value)), body, maybePagingRequest = None)

  def apply(name:          String Refined NonEmpty,
            prefixes:      Set[String Refined NonEmpty],
            body:          String,
            pagingRequest: PagingRequest
  ): SparqlQuery = SparqlQuery(name, prefixes.map(p => Prefix(p.value)), body, pagingRequest.some)

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank[Prefix] {
    def apply(name: String Refined NonEmpty, schema: Schema): Prefix = Prefix(schema asPrefix name.value)
  }

  object Prefixes {

    lazy val empty: Set[Prefix] = Set.empty

    def of(first: (Schema, String Refined NonEmpty), other: (Schema, String Refined NonEmpty)*): Set[Prefix] =
      (first +: other).map { case (schema, name) => Prefix(name, schema) }.toSet
  }

  val totalField: String = "total"

  implicit class SparqlQueryOps(sparqlQuery: SparqlQuery) {

    def include[F[_]: MonadThrow](pagingRequest: PagingRequest): F[SparqlQuery] =
      if (sparqlQuery.body.trim.matches("(?si)^.*(ORDER[ ]+BY[ ]+((ASC|DESC)[ ]*\\([ ]*\\?\\w+[ ]*\\)[ ]*)+)$"))
        sparqlQuery.copy(maybePagingRequest = Some(pagingRequest)).pure[F]
      else
        new Exception("Sparql query cannot be used for paging as there's no ending ORDER BY clause")
          .raiseError[F, SparqlQuery]

    lazy val toCountQuery: SparqlQuery = sparqlQuery.copy(
      body = s"""|SELECT (COUNT(*) AS ?$totalField)
                 |WHERE {
                 |  ${sparqlQuery.body}
                 |}""".stripMargin
    )
  }
}
