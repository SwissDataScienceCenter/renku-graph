/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import ch.datascience.config.RenkuResourcesUrl
import ch.datascience.config.sentry.SentryConfig
import ch.datascience.config.sentry.SentryConfig.{EnvironmentName, SentryBaseUrl, ServiceName}
import ch.datascience.control.{RateLimit, RateLimitUnit}
import ch.datascience.crypto.AesCrypto
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.users.{Email, Name, Username}
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import ch.datascience.http.client._
import ch.datascience.http.rest.Links
import ch.datascience.http.rest.Links.{Href, Link, Rel}
import ch.datascience.rdfstore.{DatasetName, FusekiBaseUrl, RdfStoreConfig}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.scalacheck.Gen

object CommonGraphGenerators {

  implicit val emails: Gen[Email] = for {
    beforeAt <- nonEmptyStrings()
    afterAt  <- nonEmptyStrings()
  } yield Email(s"$beforeAt@$afterAt")

  implicit val usernames: Gen[Username] = nonEmptyStrings() map Username.apply

  implicit val names: Gen[Name] = for {
    first  <- nonEmptyStrings()
    second <- nonEmptyStrings()
  } yield Name(s"$first $second")

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
    fusekiUrl       <- httpUrls map FusekiBaseUrl.apply
    datasetName     <- nonEmptyStrings() map DatasetName.apply
    authCredentials <- basicAuthCredentials
  } yield RdfStoreConfig(fusekiUrl, datasetName, authCredentials)

  implicit val schemaVersions: Gen[SchemaVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(SchemaVersion.apply)

  implicit val renkuBaseUrls:      Gen[RenkuBaseUrl]      = httpUrls map RenkuBaseUrl.apply
  implicit val renkuResourcesUrls: Gen[RenkuResourcesUrl] = httpUrls map RenkuResourcesUrl.apply

  private implicit val sentryBaseUrls: Gen[SentryBaseUrl] = for {
    url         <- httpUrls
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
    baseUrl <- httpUrls
    path    <- relativePaths()
  } yield Href(s"$baseUrl/$path")
  implicit val linkObjects: Gen[Link] = for {
    rel  <- rels
    href <- hrefs
  } yield Link(rel, href)
  implicit val linksObjects: Gen[Links] = nonEmptyList(linkObjects) map Links.apply
}
