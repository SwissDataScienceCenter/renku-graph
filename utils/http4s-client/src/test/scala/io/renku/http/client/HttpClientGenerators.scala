/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.http.client

import io.renku.generators.Generators
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.http.rest.{SortBy, Sorting, paging}
import io.renku.http.rest.paging.model.Total
import io.renku.http.rest.paging.{PagingRequest, PagingResponse}
import org.http4s.Status
import org.scalacheck.Gen

import scala.util.Try

trait HttpClientGenerators {

  implicit val pages: Gen[paging.model.Page] =
    Generators.positiveInts(max = 100) map (_.value) map paging.model.Page.apply
  implicit val perPages: Gen[paging.model.PerPage] =
    Generators.positiveInts(max = paging.model.PerPage.max.value).map(v => paging.model.PerPage(v.value))
  implicit val pagingRequests: Gen[PagingRequest] = for {
    page    <- pages
    perPage <- perPages
  } yield PagingRequest(page, perPage)
  implicit val totals: Gen[paging.model.Total] = Generators.nonNegativeInts() map (_.value) map paging.model.Total.apply

  def pagingResponses[Result](resultsGen: Gen[Result]): Gen[PagingResponse[Result]] = for {
    page    <- pages
    perPage <- perPages
    results <- Generators.listOf(resultsGen, max = perPage.value)
    total = Total((page.value - 1) * perPage.value + results.size)
  } yield PagingResponse
    .from[Try, Result](results, PagingRequest(page, perPage), total)
    .fold(throw _, identity)

  implicit lazy val sortingDirections: Gen[SortBy.Direction] = Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc)

  def sortBys[T <: SortBy](sortBy: T): Gen[Sorting[T]] = for {
    property  <- Gen.oneOf(sortBy.properties.toList)
    direction <- sortingDirections
  } yield Sorting(sortBy.By(property, direction))

  object TestSort extends SortBy {
    type PropertyType = TestProperty
    sealed trait TestProperty extends Property
    case object Name          extends Property(name = "name") with TestProperty
    case object Email         extends Property(name = "email") with TestProperty

    override val properties: Set[TestProperty] = Set(Name, Email)
  }

  def testSortBys: Gen[Sorting[TestSort.type]] = sortBys(TestSort)

  implicit val basicAuthUsernames: Gen[BasicAuthUsername] = Generators.nonEmptyStrings() map BasicAuthUsername.apply
  implicit val basicAuthPasswords: Gen[BasicAuthPassword] = Generators.nonEmptyStrings() map BasicAuthPassword.apply
  implicit val basicAuthCredentials: Gen[BasicAuthCredentials] = for {
    username <- basicAuthUsernames
    password <- basicAuthPasswords
  } yield BasicAuthCredentials(username, password)

  lazy val httpStatuses: Gen[Status] = Gen.oneOf(successHttpStatuses, clientErrorHttpStatuses, serverErrorHttpStatuses)

  lazy val successHttpStatuses: Gen[Status] = Gen.oneOf(Status.Ok, Status.Created, Status.Accepted)

  lazy val clientErrorHttpStatuses: Gen[Status] = Gen.oneOf(
    Status.Unauthorized,
    Status.PaymentRequired,
    Status.Forbidden,
    Status.NotFound,
    Status.Conflict
  )
  lazy val serverErrorHttpStatuses: Gen[Status] = Gen.oneOf(
    Status.InternalServerError,
    Status.NotImplemented,
    Status.BadGateway,
    Status.ServiceUnavailable,
    Status.GatewayTimeout
  )

  implicit val unexpectedResponseExceptions: Gen[UnexpectedResponseException] =
    unexpectedResponseExceptions(serverErrorHttpStatuses)

  def unexpectedResponseExceptions(statusesGen: Gen[Status]): Gen[UnexpectedResponseException] = for {
    status  <- statusesGen
    message <- Generators.nonBlankStrings()
  } yield UnexpectedResponseException(status, message.value)

  implicit val clientExceptions: Gen[ClientException] = for {
    message   <- Generators.nonBlankStrings()
    exception <- Generators.exceptions
  } yield ClientException(message.value, exception)

  implicit val connectivityExceptions: Gen[ConnectivityException] = for {
    message   <- Generators.nonBlankStrings()
    exception <- Generators.exceptions
  } yield ConnectivityException(message.value, exception)
}

object HttpClientGenerators extends HttpClientGenerators
