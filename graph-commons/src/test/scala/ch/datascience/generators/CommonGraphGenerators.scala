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

package ch.datascience.generators

import ch.datascience.config.certificates.Certificate

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64
import ch.datascience.config.renku
import ch.datascience.config.sentry.SentryConfig
import ch.datascience.config.sentry.SentryConfig.{EnvironmentName, SentryBaseUrl, ServiceName}
import ch.datascience.control.{RateLimit, RateLimitUnit}
import ch.datascience.crypto.AesCrypto
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.Schemas
import ch.datascience.graph.config.{GitLabUrl, RenkuBaseUrl, RenkuLogTimeout}
import ch.datascience.graph.model.CliVersion
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client._
import ch.datascience.http.rest.Links.{Href, Link, Rel}
import ch.datascience.http.rest.paging.model.Total
import ch.datascience.http.rest.paging.{PagingRequest, PagingResponse}
import ch.datascience.http.rest.{Links, SortBy, paging}
import ch.datascience.rdfstore._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.literal._
import io.renku.jsonld.Schema
import org.scalacheck.Gen

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object CommonGraphGenerators {

  implicit val aesCryptoSecrets: Gen[AesCrypto.Secret] =
    stringsOfLength(16)
      .map(_.getBytes(UTF_8))
      .map(Base64.getEncoder.encode)
      .map(new String(_, UTF_8))
      .map(Refined.unsafeApply)

  implicit val personalAccessTokens: Gen[PersonalAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield PersonalAccessToken(chars.mkString(""))

  implicit val oauthAccessTokens: Gen[OAuthAccessToken] = for {
    length <- Gen.choose(5, 40)
    chars  <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'z').map(_.toString)))
  } yield OAuthAccessToken(chars.mkString(""))

  implicit val accessTokens: Gen[AccessToken] = for {
    boolean     <- Gen.oneOf(true, false)
    accessToken <- if (boolean) personalAccessTokens else oauthAccessTokens
  } yield accessToken

  implicit val basicAuthUsernames: Gen[BasicAuthUsername] = nonEmptyStrings() map BasicAuthUsername.apply
  implicit val basicAuthPasswords: Gen[BasicAuthPassword] = nonEmptyStrings() map BasicAuthPassword.apply
  implicit val basicAuthCredentials: Gen[BasicAuthCredentials] = for {
    username <- basicAuthUsernames
    password <- basicAuthPasswords
  } yield BasicAuthCredentials(username, password)

  def rateLimits[Target]: Gen[RateLimit[Target]] =
    for {
      items <- positiveLongs()
      unit  <- Gen.oneOf(RateLimitUnit.Second, RateLimitUnit.Minute, RateLimitUnit.Hour, RateLimitUnit.Day)
    } yield RateLimit[Target](items, per = unit)

  implicit val rdfStoreConfigs: Gen[RdfStoreConfig] = for {
    fusekiUrl       <- httpUrls() map FusekiBaseUrl.apply
    datasetName     <- nonEmptyStrings() map DatasetName.apply
    authCredentials <- basicAuthCredentials
  } yield RdfStoreConfig(fusekiUrl, datasetName, authCredentials)

  implicit val cliVersions: Gen[CliVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(CliVersion.apply)

  implicit val renkuLogTimeouts: Gen[RenkuLogTimeout] = durations(max = 5 hours) map RenkuLogTimeout.apply

  implicit val renkuBaseUrls: Gen[RenkuBaseUrl] = httpUrls() map RenkuBaseUrl.apply
  implicit val renkuResourcesUrls: Gen[renku.ResourcesUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 1)
  } yield renku.ResourcesUrl(s"$url/$path")
  def renkuResourceUrls(
      renkuResourcesUrl: renku.ResourcesUrl = renkuResourcesUrls.generateOne
  ): Gen[renku.ResourceUrl] =
    for {
      path <- relativePaths(maxSegments = 1)
    } yield renkuResourcesUrl / path
  implicit val gitLabUrls: Gen[GitLabUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 2)
  } yield GitLabUrl(s"$url/$path")

  private implicit val sentryBaseUrls: Gen[SentryBaseUrl] = for {
    url         <- httpUrls()
    projectName <- nonEmptyList(nonEmptyStrings()).map(_.toList.mkString("."))
    projectId   <- positiveInts(max = 100)
  } yield SentryBaseUrl(s"$url@$projectName/$projectId")
  private implicit val serviceNames:     Gen[ServiceName]     = nonEmptyStrings() map ServiceName.apply
  private implicit val environmentNames: Gen[EnvironmentName] = nonEmptyStrings() map EnvironmentName.apply
  implicit val sentryConfigs: Gen[SentryConfig] = for {
    url             <- sentryBaseUrls
    serviceName     <- serviceNames
    environmentName <- environmentNames
  } yield SentryConfig(url, environmentName, serviceName)

  implicit val rels: Gen[Rel] = nonEmptyStrings() map Rel.apply
  implicit val hrefs: Gen[Href] = for {
    baseUrl <- httpUrls()
    path    <- relativePaths()
  } yield Href(s"$baseUrl/$path")
  implicit val linkObjects: Gen[Link] = for {
    rel  <- rels
    href <- hrefs
  } yield Link(rel, href)
  implicit val linksObjects: Gen[Links] = nonEmptyList(linkObjects) map Links.apply

  def sortBys[T <: SortBy](sortBy: T): Gen[T#By] =
    for {
      property  <- Gen.oneOf(sortBy.properties.toList)
      direction <- Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc)
    } yield sortBy.By(property, direction)

  object TestSort extends SortBy {
    type PropertyType = TestProperty
    sealed trait TestProperty extends Property
    case object Name          extends Property(name = "name") with TestProperty
    case object Email         extends Property(name = "email") with TestProperty

    override val properties: Set[TestProperty] = Set(Name, Email)
  }

  def testSortBys: Gen[TestSort.By] = sortBys(TestSort)

  implicit val pages:    Gen[paging.model.Page]    = positiveInts(max = 100) map (_.value) map paging.model.Page.apply
  implicit val perPages: Gen[paging.model.PerPage] = positiveInts(max = 20) map (_.value) map paging.model.PerPage.apply
  implicit val pagingRequests: Gen[PagingRequest] = for {
    page    <- pages
    perPage <- perPages
  } yield PagingRequest(page, perPage)
  implicit val totals: Gen[paging.model.Total] = nonNegativeInts() map (_.value) map paging.model.Total.apply

  def pagingResponses[Result](resultsGen: Gen[Result]): Gen[PagingResponse[Result]] =
    for {
      page    <- pages
      perPage <- perPages
      results <- listOf(resultsGen, maxElements = Refined.unsafeApply(perPage.value))
      total = Total((page.value - 1) * perPage.value + results.size)
    } yield PagingResponse
      .from[Try, Result](results, PagingRequest(page, perPage), total)
      .fold(throw _, identity)

  implicit val fusekiBaseUrls: Gen[FusekiBaseUrl] = httpUrls() map FusekiBaseUrl.apply

  implicit lazy val certificates: Gen[Certificate] =
    nonBlankStrings()
      .toGeneratorOfNonEmptyList(minElements = 2)
      .map { lines =>
        Certificate {
          lines.toList.mkString("-----BEGIN CERTIFICATE-----\n", "\n", "\n-----END CERTIFICATE-----")
        }
      }

  implicit val jsonLDTriples: Gen[JsonLDTriples] = for {
    subject <- nonEmptyStrings()
    obj     <- nonEmptyStrings()
  } yield JsonLDTriples {
    json"""{
        "@context": {
          "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
          "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
          "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@id": $subject,
        "rdfs:label": $obj
      }"""
  }

  implicit lazy val sparqlPrefixes: Gen[SparqlQuery.Prefix] = Gen.oneOf(
    SparqlQuery.Prefix("prov", Schemas.prov),
    SparqlQuery.Prefix("wfprov", Schemas.wfprov),
    SparqlQuery.Prefix("wfdesc", Schemas.wfdesc),
    SparqlQuery.Prefix("rdf", Schemas.rdf),
    SparqlQuery.Prefix("rdfs", Schemas.rdfs),
    SparqlQuery.Prefix("xmlSchema", Schemas.xmlSchema),
    SparqlQuery.Prefix("schema", Schemas.schema),
    SparqlQuery.Prefix("renku", Schemas.renku)
  )

  implicit lazy val sparqlQueries: Gen[SparqlQuery] = for {
    sparqlQuery <- sentences() map (v => SparqlQuery("curation update", Set.empty[String Refined NonEmpty], v.value))
  } yield sparqlQuery

  implicit lazy val schemas: Gen[Schema] = Gen.oneOf(Schemas.all)
}
