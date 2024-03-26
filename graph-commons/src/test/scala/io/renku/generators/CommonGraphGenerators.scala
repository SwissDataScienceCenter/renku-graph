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

package io.renku.generators

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.renku.config._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GraphModelGenerators.personGitLabIds
import io.renku.graph.model.Schemas
import io.renku.http.client._
import io.renku.http.rest.Links
import io.renku.http.rest.Links.{Href, Link, Rel}
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.{AuthenticationFailure, AuthorizationFailure}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore._
import org.scalacheck.Gen

object CommonGraphGenerators {

  implicit val securityExceptions: Gen[EndpointSecurityException] =
    Gen.oneOf(AuthenticationFailure, AuthorizationFailure)

  val datasetConfigFiles: Gen[DatasetConfigFile] =
    (nonEmptyStrings(), nonEmptyStrings()).mapN { case (dsName, body) =>
      new DatasetConfigFile {
        override val datasetName: DatasetName = DatasetName(dsName)
        override val value:       String      = body
      }
    }

  implicit val adminConnectionConfigs: Gen[AdminConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    credentials <- HttpClientGenerators.basicAuthCredentials
  } yield AdminConnectionConfig(url, credentials)

  final case class TestDatasetConnectionConfig(fusekiUrl:       FusekiUrl,
                                               datasetName:     DatasetName,
                                               authCredentials: BasicAuthCredentials
  ) extends DatasetConnectionConfig

  implicit val storeConnectionConfigs: Gen[TestDatasetConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    name        <- nonEmptyStrings().toGeneratorOf(DatasetName)
    credentials <- HttpClientGenerators.basicAuthCredentials
  } yield TestDatasetConnectionConfig(url, name, credentials)

  implicit val datasetConnectionConfigs: Gen[DatasetConnectionConfig] = for {
    url         <- httpUrls() map FusekiUrl.apply
    dataset     <- nonEmptyStrings().toGeneratorOf(DatasetName)
    credentials <- HttpClientGenerators.basicAuthCredentials
  } yield new DatasetConnectionConfig {
    override val fusekiUrl       = url
    override val datasetName     = dataset
    override val authCredentials = credentials
  }

  implicit val renkuApiUrls: Gen[renku.ApiUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 1)
  } yield renku.ApiUrl(s"$url/$path")

  def renkuResourceUrls(renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne): Gen[renku.ResourceUrl] =
    relativePaths(maxSegments = 1) map (path => renkuApiUrl / path)

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

  implicit val fusekiUrls: Gen[FusekiUrl] = httpUrls() map FusekiUrl.apply

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

  implicit val serviceUrls: Gen[ServiceUrl] = httpUrls() map ServiceUrl.apply

  implicit val authUsers: Gen[AuthUser] = for {
    gitLabId    <- personGitLabIds
    accessToken <- GitLabGenerators.userAccessTokens
  } yield AuthUser(gitLabId, accessToken)

  implicit def authContexts[Key](implicit keysGen: Gen[Key]): Gen[AuthContext[Key]] = for {
    maybeAuthUser <- authUsers.toGeneratorOfOptions
    key           <- keysGen
  } yield AuthContext(maybeAuthUser, key)
}
