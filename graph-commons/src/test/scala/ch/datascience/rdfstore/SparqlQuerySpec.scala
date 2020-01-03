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

package ch.datascience.rdfstore

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Failure, Success, Try}

class SparqlQuerySpec extends WordSpec {

  "toString" should {

    "render the body only if no prefixes and paging request given" in {
      val body = sentences().generateOne.value

      SparqlQuery(Set.empty, body).toString shouldBe body
    }

    "render the prefixes and body only if no paging request given" in {
      val prefix1 = nonBlankStrings().generateOne
      val prefix2 = nonBlankStrings().generateOne
      val body    = sentences().generateOne.value

      SparqlQuery(Set(prefix1, prefix2), body).toString should (
        equal(
          s"""|$prefix1
              |$prefix2
              |$body""".stripMargin
        ) or equal(
          s"""|$prefix2
              |$prefix1
              |$body""".stripMargin
        )
      )
    }

    "render the prefixes, body and paging request" in {
      val prefix = nonBlankStrings().generateOne
      val body =
        s"""|${sentences().generateOne.value}
            |ORDER BY ASC(?field)""".stripMargin
      val pagingRequest = pagingRequests.generateOne

      val Success(query) = SparqlQuery(Set(prefix), body).include[Try](pagingRequest)

      query.toString shouldBe
        s"""|$prefix
            |$body
            |LIMIT ${pagingRequest.perPage}
            |OFFSET ${(pagingRequest.page.value - 1) * pagingRequest.perPage.value}""".stripMargin
    }
  }

  "include" should {

    "successfully add the given paging request to the SparqlQuery if there's 'ORDER BY' clause in the body" in {
      val query = SparqlQuery(
        prefixes = Set.empty,
        body     = s"""|${sentences().generateOne.value}
                   |ORDER BY ASC(?field)
                   |""".stripMargin
      )
      val pagingRequest = pagingRequests.generateOne

      query.include[Try](pagingRequest) shouldBe SparqlQuery(query.prefixes, query.body, Some(pagingRequest)).pure[Try]
    }

    "successfully add the given paging request to the SparqlQuery if there's 'order by' clause in the body" in {
      val query = SparqlQuery(
        prefixes = Set.empty,
        body     = s"""|${sentences().generateOne.value}
                   |order by asc(?field)
                   |""".stripMargin
      )
      val pagingRequest = pagingRequests.generateOne

      query.include[Try](pagingRequest) shouldBe SparqlQuery(query.prefixes, query.body, Some(pagingRequest)).pure[Try]
    }

    "fail adding the given paging request if there's no ORDER BY clause in the body" in {
      val query         = SparqlQuery(prefixes = Set.empty, body = sentences().generateOne.value)
      val pagingRequest = pagingRequests.generateOne

      val Failure(exception) = query.include[Try](pagingRequest)

      exception.getMessage shouldBe "Sparql query cannot be used for paging as there's no ending ORDER BY clause"
    }
  }

  "toCountQuery" should {

    "wrap query's body with the COUNT clause" in {
      val prefixes = Set(nonBlankStrings().generateOne)
      val body     = sentences().generateOne.value
      val query    = SparqlQuery(prefixes, body)

      val actual = query.toCountQuery

      actual.body shouldBe
        s"""|SELECT (COUNT(*) AS ?${SparqlQuery.totalField})
            |WHERE {
            |  $body
            |}""".stripMargin
      actual.prefixes           shouldBe prefixes
      actual.maybePagingRequest shouldBe None
    }
  }
}
