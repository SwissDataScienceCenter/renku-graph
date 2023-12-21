/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.generators

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.config._
import io.renku.config.certificates.Certificate
import io.renku.config.sentry.SentryConfig
import io.renku.config.sentry.SentryConfig.{Dsn, Environment}
import io.renku.control.{RateLimit, RateLimitUnit}
import io.renku.crypto.AesCrypto.Secret
import io.renku.data.Message
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.Schemas
import io.renku.http.client.AccessToken._
import io.renku.http.client.RestClientError._
import io.renku.http.client._
import io.renku.http.rest.Links.{Href, Link, Rel}
import io.renku.http.rest.paging.model.Total
import io.renku.http.rest.paging.{PagingRequest, PagingResponse}
import io.renku.http.rest.{Links, SortBy, Sorting, paging}
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.{AuthenticationFailure, AuthorizationFailure}
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.Schema
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier}
import io.renku.triplesstore._
import org.http4s.Status
import org.http4s.Status._
import org.scalacheck.Gen.asciiPrintableChar
import org.scalacheck.{Arbitrary, Gen}
import scodec.bits.ByteVector

import scala.language.implicitConversions
import scala.util.Try

object CommonGraphGenerators {

  implicit val aesCryptoSecrets: Gen[Secret] =
    Gen
      .oneOf(16, 24, 32)
      .flatMap { length =>
        Gen
          .listOfN(length, asciiPrintableChar)
          .map(_.mkString)
          .map(_.getBytes("US-ASCII"))
          .map(ByteVector(_))
          .map(Secret.unsafe)
      }

  implicit val personalAccessTokens: Gen[PersonalAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield PersonalAccessToken(chars.mkString(""))

  implicit val userOAuthAccessTokens: Gen[UserOAuthAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield UserOAuthAccessToken(chars.mkString(""))

  implicit val projectAccessTokens: Gen[ProjectAccessToken] = for {
    chars <- Gen.listOfN(20, Gen.oneOf(('A' to 'Z').map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield ProjectAccessToken(s"$ProjectAccessTokenDefaultPrefix${chars.mkString("")}")

  implicit val securityExceptions: Gen[EndpointSecurityException] =
    Gen.oneOf(AuthenticationFailure, AuthorizationFailure)

  implicit val userAccessTokens: Gen[UserAccessToken] = Gen.oneOf(userOAuthAccessTokens, personalAccessTokens)

  implicit val accessTokens: Gen[AccessToken] =
    Gen.oneOf(projectAccessTokens, userOAuthAccessTokens, personalAccessTokens)

  implicit val basicAuthUsernames: Gen[BasicAuthUsername] = nonEmptyStrings() map BasicAuthUsername.apply
  implicit val basicAuthPasswords: Gen[BasicAuthPassword] = nonEmptyStrings() map BasicAuthPassword.apply
  implicit val basicAuthCredentials: Gen[BasicAuthCredentials] = for {
    username <- basicAuthUsernames
    password <- basicAuthPasswords
  } yield BasicAuthCredentials(username, password)

  def rateLimits[Target]: Gen[RateLimit[Target]] = for {
    items <- positiveLongs()
    unit  <- Gen.oneOf(RateLimitUnit.Second, RateLimitUnit.Minute, RateLimitUnit.Hour, RateLimitUnit.Day)
  } yield RateLimit[Target](items, per = unit)

  val datasetConfigFiles: Gen[DatasetConfigFile] = nonEmptyStrings().map { v =>
    new DatasetConfigFile {
      override lazy val value: String = v
    }
  }

  implicit val adminConnectionConfigs: Gen[AdminConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    credentials <- basicAuthCredentials
  } yield AdminConnectionConfig(url, credentials)

  final case class TestDatasetConnectionConfig(fusekiUrl:       FusekiUrl,
                                               datasetName:     DatasetName,
                                               authCredentials: BasicAuthCredentials
  ) extends DatasetConnectionConfig

  implicit val storeConnectionConfigs: Gen[TestDatasetConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    name        <- nonEmptyStrings().toGeneratorOf(DatasetName)
    credentials <- basicAuthCredentials
  } yield new TestDatasetConnectionConfig(url, name, credentials)

  implicit val datasetConnectionConfigs: Gen[DatasetConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    dataset     <- nonEmptyStrings().toGeneratorOf(DatasetName)
    credentials <- basicAuthCredentials
  } yield new DatasetConnectionConfig {
    override val fusekiUrl       = url
    override val datasetName     = dataset
    override val authCredentials = credentials
  }

  implicit val microserviceBaseUrls: Gen[MicroserviceBaseUrl] = for {
    protocol <- Arbitrary.arbBool.arbitrary map {
                  case true  => "http"
                  case false => "https"
                }
    port <- httpPorts
    ip1  <- positiveInts(999)
    ip2  <- positiveInts(999)
    ip3  <- positiveInts(999)
    ip4  <- positiveInts(999)
  } yield MicroserviceBaseUrl(s"$protocol://$ip1$ip2$ip3$ip4:$port")

  implicit val microserviceIdentifiers: Gen[MicroserviceIdentifier] =
    Gen.uuid map (_ => MicroserviceIdentifier.generate)

  implicit lazy val serviceNames: Gen[ServiceName] = nonEmptyStrings().toGeneratorOf(ServiceName)

  implicit lazy val serviceVersions: Gen[ServiceVersion] = for {
    version       <- semanticVersions
    commitsNumber <- positiveInts(999)
    commitPart    <- shas.toGeneratorOfOptions.map(_.map(_.take(8)).map(sha => s"-$commitsNumber-g$sha").getOrElse(""))
  } yield ServiceVersion(s"$version$commitPart")

  implicit val renkuApiUrls: Gen[renku.ApiUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 1)
  } yield renku.ApiUrl(s"$url/$path")

  def renkuResourceUrls(renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne): Gen[renku.ResourceUrl] =
    relativePaths(maxSegments = 1) map (path => renkuApiUrl / path)

  private implicit val sentryDsns: Gen[Dsn] = for {
    url         <- httpUrls()
    projectName <- nonEmptyList(nonEmptyStrings()).map(_.toList.mkString("."))
    projectId   <- positiveInts(max = 100)
  } yield Dsn(s"$url@$projectName/$projectId")
  private implicit val sentryEnvironments: Gen[Environment] = nonEmptyStrings() map Environment.apply
  implicit val sentryConfigs: Gen[SentryConfig] = for {
    dsn            <- sentryDsns
    environment    <- sentryEnvironments
    serviceName    <- serviceNames
    serviceVersion <- serviceVersions
  } yield SentryConfig(dsn, environment, serviceName, serviceVersion)

  implicit val rels:        Gen[Rel]          = nonEmptyStrings() map Rel.apply
  implicit val linkMethods: Gen[Links.Method] = Gen.oneOf(Links.Method.all)
  implicit val hrefs: Gen[Href] = for {
    baseUrl <- httpUrls()
    path    <- relativePaths()
  } yield Href(s"$baseUrl/$path")
  implicit val linkObjects: Gen[Link] = for {
    rel    <- rels
    href   <- hrefs
    method <- linkMethods
  } yield Link(rel, href, method)
  implicit val linksObjects: Gen[Links] = nonEmptyList(linkObjects) map Links.apply

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

  implicit val pages: Gen[paging.model.Page] = positiveInts(max = 100) map (_.value) map paging.model.Page.apply
  implicit val perPages: Gen[paging.model.PerPage] =
    positiveInts(max = paging.model.PerPage.max.value).map(v => paging.model.PerPage(v.value))
  implicit val pagingRequests: Gen[PagingRequest] = for {
    page    <- pages
    perPage <- perPages
  } yield PagingRequest(page, perPage)
  implicit val totals: Gen[paging.model.Total] = nonNegativeInts() map (_.value) map paging.model.Total.apply

  def pagingResponses[Result](resultsGen: Gen[Result]): Gen[PagingResponse[Result]] = for {
    page    <- pages
    perPage <- perPages
    results <- listOf(resultsGen, max = perPage.value)
    total = Total((page.value - 1) * perPage.value + results.size)
  } yield PagingResponse
    .from[Try, Result](results, PagingRequest(page, perPage), total)
    .fold(throw _, identity)

  implicit val fusekiUrls: Gen[FusekiUrl] = httpUrls() map FusekiUrl.apply

  implicit lazy val certificates: Gen[Certificate] =
    nonBlankStrings()
      .toGeneratorOfNonEmptyList(min = 2)
      .map { lines =>
        Certificate {
          lines.toList.mkString("-----BEGIN CERTIFICATE-----\n", "\n", "\n-----END CERTIFICATE-----")
        }
      }

  implicit lazy val sparqlPrefixes: Gen[SparqlQuery.Prefix] = Gen.oneOf(
    SparqlQuery.Prefix("prov", Schemas.prov),
    SparqlQuery.Prefix("wfprov", Schemas.wfprov),
    SparqlQuery.Prefix("wfdesc", Schemas.wfdesc),
    SparqlQuery.Prefix("rdf", Schemas.rdf),
    SparqlQuery.Prefix("rdfs", Schemas.rdfs),
    SparqlQuery.Prefix("xsd", Schemas.xsd),
    SparqlQuery.Prefix("schema", Schemas.schema),
    SparqlQuery.Prefix("renku", Schemas.renku)
  )

  implicit lazy val sparqlQueries: Gen[SparqlQuery] = for {
    sparqlQuery <- sentences() map (v => SparqlQuery("curation update", Set.empty[String Refined NonEmpty], v.value))
  } yield sparqlQuery

  implicit lazy val schemas: Gen[Schema] = Gen.oneOf(Schemas.all)

  lazy val httpStatuses: Gen[Status] = Gen.oneOf(successHttpStatuses, clientErrorHttpStatuses, serverErrorHttpStatuses)

  lazy val successHttpStatuses: Gen[Status] = Gen.oneOf(Ok, Created, Accepted)

  lazy val clientErrorHttpStatuses: Gen[Status] = Gen.oneOf(
    Unauthorized,
    PaymentRequired,
    Forbidden,
    NotFound,
    Conflict
  )
  lazy val serverErrorHttpStatuses: Gen[Status] = Gen.oneOf(
    InternalServerError,
    NotImplemented,
    BadGateway,
    ServiceUnavailable,
    GatewayTimeout
  )

  implicit val unexpectedResponseExceptions: Gen[UnexpectedResponseException] =
    unexpectedResponseExceptions(serverErrorHttpStatuses)

  implicit def unexpectedResponseExceptions(statusesGen: Gen[Status]): Gen[UnexpectedResponseException] = for {
    status  <- statusesGen
    message <- nonBlankStrings()
  } yield UnexpectedResponseException(status, message.value)

  implicit val connectivityExceptions: Gen[ConnectivityException] = for {
    message   <- nonBlankStrings()
    exception <- exceptions
  } yield ConnectivityException(message.value, exception)

  implicit val clientExceptions: Gen[ClientException] = for {
    message   <- nonBlankStrings()
    exception <- exceptions
  } yield ClientException(message.value, exception)

  implicit val serviceUrls:  Gen[ServiceUrl]  = httpUrls() map ServiceUrl.apply
  implicit val elapsedTimes: Gen[ElapsedTime] = Gen.choose(0L, 10000L) map ElapsedTime.apply

  implicit val authUsers: Gen[AuthUser] = for {
    gitLabId    <- personGitLabIds
    accessToken <- userAccessTokens
  } yield AuthUser(gitLabId, accessToken)

  implicit def authContexts[Key](implicit keysGen: Gen[Key]): Gen[AuthContext[Key]] = for {
    maybeAuthUser <- authUsers.toGeneratorOfOptions
    key           <- keysGen
  } yield AuthContext(maybeAuthUser, key)

  val errorMessages: Gen[Message] = Gen.oneOf(
    nonBlankStrings().map(Message.Error(_)),
    exceptions.map(Message.Error.fromExceptionMessage(_)),
    jsons.map(Message.Error.fromJsonUnsafe)
  )

  val infoMessages: Gen[Message] =
    nonBlankStrings().map(Message.Info(_))

  implicit val messages: Gen[Message] =
    Gen.oneOf(errorMessages, infoMessages)
}
