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

package io.renku.knowledgegraph

import cats.data.EitherT.{leftT, rightT}
import cats.data.{EitherT, Kleisli}
import cats.effect.{IO, Resource}
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Json
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.RenkuUrl
import io.renku.http.client.UrlEncoder.urlEncode
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.PagingRequest
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.rest.{SortBy, Sorting}
import io.renku.http.server.EndpointTester._
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.{AuthUser, MaybeAuthUser}
import io.renku.http.server.version
import io.renku.http.tinytypes.TinyTypeURIEncoder._
import io.renku.interpreters.TestRoutesMetrics
import io.renku.knowledgegraph.datasets.details.RequestedDataset
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Method.{DELETE, GET, PATCH, POST, PUT}
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.http4s.server.AuthMiddleware
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import projects.files.lineage.LineageGenerators._
import projects.files.lineage.model.Node.Location

import scala.language.reflectiveCalls

class MicroserviceRoutesSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "GET /knowledge-graph/datasets?query=<phrase>" should {

    import datasets.Endpoint.Query._
    import datasets.Endpoint.Sort
    import datasets.Endpoint.Sort._
    import datasets._

    s"return $Ok when a valid 'query' and no 'sort', `page` and `per_page` parameters given" in new TestCase {

      val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)
      val phrase        = nonEmptyStrings().generateOne
      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
        .expects(Phrase(phrase).some,
                 Sorting(Sort.By(NameProperty, Direction.Asc)),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser.option
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser)
        .call(Request[IO](GET, uri"/knowledge-graph/datasets".withQueryParam(query.parameterName, phrase)))
        .status shouldBe Ok
    }

    s"return $Ok when no ${query.parameterName} parameter given" in new TestCase {

      val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)

      (datasetsSearchEndpoint
        .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
        .expects(Option.empty[Phrase],
                 Sorting(Sort.By(NameProperty, Direction.Asc)),
                 PagingRequest(Page.first, PerPage.default),
                 maybeAuthUser.option
        )
        .returning(IO.pure(Response[IO](Ok)))

      routes(maybeAuthUser).call(Request[IO](GET, uri"/knowledge-graph/datasets")).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/datasets"))
        .status shouldBe Unauthorized
    }

    Sort.properties foreach { sortProperty =>
      val sortBy = Sort.By(sortProperty, Gen.oneOf(SortBy.Direction.Asc, SortBy.Direction.Desc).generateOne)

      s"return $Ok when '${query.parameterName}' and 'sort=${sortBy.property}:${sortBy.direction}' parameters given" in new TestCase {
        val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)

        val phrase = phrases.generateOne
        val request = Request[IO](
          GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("sort", s"${sortBy.property}:${sortBy.direction}")
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some, Sorting(sortBy), PagingRequest.default, maybeAuthUser.option)
          .returning(IO.pure(Response[IO](Ok)))

        val response = routes(maybeAuthUser).call(request)

        response.status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }

    s"return $Ok when query, ${PagingRequest.Decoders.page.parameterName} and ${PagingRequest.Decoders.perPage.parameterName} parameters given" in new TestCase {
      forAll(phrases, pages, perPages) { (phrase, page, perPage) =>
        val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)

        val request = Request[IO](
          GET,
          uri"/knowledge-graph/datasets"
            .withQueryParam("query", phrase.value)
            .withQueryParam("page", page.value)
            .withQueryParam("per_page", perPage.value)
        )
        (datasetsSearchEndpoint
          .searchForDatasets(_: Option[Phrase], _: Sorting[Sort.type], _: PagingRequest, _: Option[AuthUser]))
          .expects(phrase.some,
                   Sorting(Sort.By(NameProperty, Direction.Asc)),
                   PagingRequest(page, perPage),
                   maybeAuthUser.option
          )
          .returning(IO.pure(Response[IO](Ok)))

        routes(maybeAuthUser).call(request).status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }

    s"return $BadRequest for invalid " +
      s"${query.parameterName} parameter, " +
      s"${Sort.sort.parameterName} parameter, " +
      s"${PagingRequest.Decoders.page.parameterName} parameter and " +
      s"${PagingRequest.Decoders.perPage.parameterName} parameter" in new TestCase {

        val sortProperty     = "invalid"
        val requestedPage    = nonPositiveInts().generateOne
        val requestedPerPage = nonPositiveInts().generateOne

        val response = routes().call {
          Request(
            GET,
            uri"/knowledge-graph/datasets"
              .withQueryParam("query", blankStrings().generateOne)
              .withQueryParam("sort", s"$sortProperty:${Direction.Asc}")
              .withQueryParam("page", requestedPage.toString)
              .withQueryParam("per_page", requestedPerPage.toString)
          )
        }

        response.status shouldBe BadRequest
        response.body[Message] shouldBe Message.Error.unsafeApply(
          List(
            s"'${query.parameterName}' parameter with invalid value",
            Sort.sort.errorMessage(sortProperty),
            PagingRequest.Decoders.page.errorMessage(requestedPage.value.toString),
            PagingRequest.Decoders.perPage.errorMessage(requestedPerPage.value.toString)
          ).mkString("; ")
        )
      }
  }

  "GET /knowledge-graph/datasets/:id" should {

    forAll {
      Table(
        "requested DS id" -> "id",
        "DS identifier"   -> RequestedDataset(datasetIdentifiers.generateOne),
        "DS sameAs"       -> RequestedDataset(datasetSameAs.generateOne)
      )
    } { (dsIdType, requestedDS) =>
      s"return $Ok when a valid $dsIdType given" in new TestCase {

        val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)

        val authContext = AuthContext(maybeAuthUser.option, requestedDS)
        givenDSAuthorizer(
          requestedDS,
          maybeAuthUser.option,
          returning = rightT[IO, EndpointSecurityException](authContext)
        )

        (datasetDetailsEndpoint.`GET /datasets/:id` _)
          .expects(requestedDS, authContext)
          .returning(Response[IO](Ok).pure[IO])

        routes(maybeAuthUser)
          .call(Request(GET, uri"/knowledge-graph/datasets" / requestedDS))
          .status shouldBe Ok
      }
    }

    s"return $NotFound when no :id path parameter given" in new TestCase {
      routes()
        .call(Request(GET, uri"/knowledge-graph/datasets/"))
        .status shouldBe NotFound
    }

    s"return $Unauthorized when user authentication failed" in new TestCase {
      routes(givenAuthAsUnauthorized)
        .call(Request(GET, uri"/knowledge-graph/datasets" / RequestedDataset(datasetIdentifiers.generateOne)))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when user has no rights for the project the dataset belongs to" in new TestCase {

      val id            = datasetIdentifiers.generateOne
      val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)

      givenDSIdAuthorizer(
        id,
        maybeAuthUser.option,
        returning = leftT[IO, AuthContext[model.datasets.Identifier]](AuthorizationFailure)
      )

      val response = routes(maybeAuthUser).call(Request(GET, uri"/knowledge-graph/datasets" / RequestedDataset(id)))

      response.status        shouldBe NotFound
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error.unsafeApply(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/entities" should {
    import io.renku.entities.search.Criteria
    import io.renku.entities.search.Criteria.Sort._
    import io.renku.entities.search.Criteria._
    import io.renku.entities.search.Generators._

    forAll {
      Table(
        "uri"                          -> "criteria",
        uri"/knowledge-graph/entities" -> Criteria(),
        queryParams
          .map(q => uri"/knowledge-graph/entities" +? ("query" -> q.value) -> Criteria(Filters(maybeQuery = q.some)))
          .generateOne,
        typeParams
          .map(t => uri"/knowledge-graph/entities" +? ("type" -> t.value) -> Criteria(Filters(entityTypes = Set(t))))
          .generateOne,
        typeParams
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("type" -> list.map(_.show))
            uri -> Criteria(Filters(entityTypes = list.toSet))
          }
          .generateOne,
        personNames
          .map(name =>
            uri"/knowledge-graph/entities" +? ("creator" -> name.value) -> Criteria(Filters(creators = Set(name)))
          )
          .generateOne,
        personNames
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("creator" -> list.map(_.show))
            uri -> Criteria(Filters(creators = list.toSet))
          }
          .generateOne,
        projectVisibilities
          .map(v =>
            uri"/knowledge-graph/entities" +? ("visibility" -> v.value) -> Criteria(Filters(visibilities = Set(v)))
          )
          .generateOne,
        projectVisibilities
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("visibility" -> list.map(_.show))
            uri -> Criteria(Filters(visibilities = list.toSet))
          }
          .generateOne,
        projectNamespaces
          .map(v =>
            uri"/knowledge-graph/entities" +? ("namespace" -> v.value) -> Criteria(Filters(namespaces = Set(v)))
          )
          .generateOne,
        projectNamespaces
          .toGeneratorOfList(min = 2)
          .map { list =>
            val uri = uri"/knowledge-graph/entities" ++? ("namespace" -> list.map(_.show))
            uri -> Criteria(Filters(namespaces = list.toSet))
          }
          .generateOne,
        sinceParams
          .map(d =>
            uri"/knowledge-graph/entities" +? ("since" -> d.value.toString) -> Criteria(Filters(maybeSince = d.some))
          )
          .generateOne,
        untilParams
          .map(d =>
            uri"/knowledge-graph/entities" +? ("until" -> d.value.toString) -> Criteria(Filters(maybeUntil = d.some))
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"matchingScore:$dir") -> Criteria(sorting =
              Sorting(Sort.By(Sort.ByMatchingScore, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"name:$dir") -> Criteria(sorting =
              Sorting(Sort.By(ByName, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" +? ("sort" -> s"date:$dir") -> Criteria(sorting =
              Sorting(Sort.By(ByDate, dir))
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            uri"/knowledge-graph/entities" ++? ("sort" -> Seq(s"date:$dir", s"name:$dir")) -> Criteria(
              sorting = Sorting(Sort.By(ByDate, dir), Sort.By(ByName, dir))
            )
          )
          .generateOne,
        pages
          .map(page =>
            uri"/knowledge-graph/entities" +? ("page" -> page.show) -> Criteria(paging =
              PagingRequest.default.copy(page = page)
            )
          )
          .generateOne,
        perPages
          .map(perPage =>
            uri"/knowledge-graph/entities" +? ("per_page" -> perPage.show) -> Criteria(paging =
              PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the query parameters from $uri, pass them to the endpoint and return received response" in new TestCase {

        val request = Request[IO](GET, uri)

        val responseBody = jsons.generateOne
        (entitiesEndpoint.`GET /entities` _)
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    "read the 'role' query parameter from the uri and pass it to the endpoint when auth user present" in new TestCase {

      val authUser      = authUsers.generateOne
      val maybeAuthUser = MaybeAuthUser(authUser)
      val role          = projectRoles.generateOne
      val criteria      = Criteria(Filters(roles = Set(role)), maybeUser = maybeAuthUser.option)
      val request       = Request[IO](GET, uri"/knowledge-graph/entities" +? ("role" -> role))

      val responseBody = jsons.generateOne
      (entitiesEndpoint.`GET /entities` _)
        .expects(criteria, request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      val response = routes(maybeAuthUser).call(request)

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.body[Json]  shouldBe responseBody
    }

    "read multiple 'role' query parameters from the uri and pass it to the endpoint when auth user present" in new TestCase {

      val authUser      = authUsers.generateOne
      val maybeAuthUser = MaybeAuthUser(authUser)
      val roles         = projectRoles.generateSet(min = 2)
      val criteria      = Criteria(Filters(roles = roles), maybeUser = maybeAuthUser.option)
      val request       = Request[IO](GET, uri"/knowledge-graph/entities" ++? ("role" -> roles.toList.map(_.show)))

      val responseBody = jsons.generateOne
      (entitiesEndpoint.`GET /entities` _)
        .expects(criteria, request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      val response = routes(maybeAuthUser).call(request)

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.body[Json]  shouldBe responseBody
    }

    s"return $BadRequest when the 'role' query parameter given but no auth user present" in new TestCase {

      val response =
        routes().call(Request[IO](GET, uri"/knowledge-graph/entities" +? ("role" -> projectRoles.generateOne)))

      response.status        shouldBe BadRequest
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error("'role' parameter present but no access token")
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {
      val response = routes()
        .call(
          Request[IO](GET, uri"/knowledge-graph/entities" +? ("visibility" -> nonEmptyStrings().generateOne))
        )

      response.status        shouldBe BadRequest
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error("'visibility' parameter with invalid value")
    }

    s"return $BadRequest for if since > until" in new TestCase {
      val since = sinceParams.generateOne
      val until = localDates(max = since.value.minusDays(1)).generateAs(Filters.Until)
      val response = routes().call {
        Request[IO](GET, uri"/knowledge-graph/entities" +? ("since" -> since.show) +? ("until" -> until.show))
      }

      response.status        shouldBe BadRequest
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error("'since' parameter > 'until'")
    }

    "authenticate user from the request if given" in new TestCase {
      val maybeAuthUser = MaybeAuthUser.apply(authUsers.generateOption)
      val request       = Request[IO](GET, uri"/knowledge-graph/entities")

      val responseBody = jsons.generateOne
      (entitiesEndpoint.`GET /entities` _)
        .expects(Criteria(maybeUser = maybeAuthUser.option), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/entities"))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/ontology" should {

    "return generated ontology" in new TestCase {

      val request  = Request[IO](GET, uri"/knowledge-graph/ontology")
      val response = Response[IO](httpStatuses.generateOne)
      (ontologyEndpoint
        .`GET /ontology`(_: Uri.Path)(_: Request[IO]))
        .expects(Uri.Path.empty, request)
        .returning(response.pure[IO])

      routes().call(request).status shouldBe response.status
    }
  }

  "POST /knowledge-graph/projects" should {

    val request = Request[IO](POST, uri"knowledge-graph/projects")

    s"return $Accepted for valid path parameters, user and payload" in new TestCase {

      val authUser = MaybeAuthUser(authUsers.generateOne)

      (projectCreateEndpoint
        .`POST /projects`(_: Request[IO], _: AuthUser))
        .expects(request, authUser.option.get)
        .returning(Response[IO](Accepted).pure[IO])

      routes(authUser).call(request).status shouldBe Accepted

      routesMetrics.clearRegistry()
    }

    s"return $Unauthorized when authentication fails" in new TestCase {
      routes(givenAuthAsUnauthorized)
        .call(request)
        .status shouldBe Unauthorized
    }

    s"return $NotFound when no auth header" in new TestCase {
      routes(maybeAuthUser = MaybeAuthUser.noUser).call(request).status shouldBe NotFound
    }
  }

  "DELETE /knowledge-graph/projects/:namespace/../:name" should {

    val projectSlug = projectSlugs.generateOne
    val request     = Request[IO](DELETE, Uri.unsafeFromString(s"knowledge-graph/projects/$projectSlug"))

    s"return $Accepted for valid path parameters and user" in new TestCase {

      val authUser = MaybeAuthUser.apply(authUsers.generateOne)

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, authUser.option)
        .returning(rightT[IO, EndpointSecurityException](AuthContext(authUser.option, projectSlug)))

      (projectDeleteEndpoint
        .`DELETE /projects/:slug`(_: model.projects.Slug, _: AuthUser))
        .expects(projectSlug, authUser.option.get)
        .returning(Response[IO](Accepted).pure[IO])

      routes(authUser).call(request).status shouldBe Accepted

      routesMetrics.clearRegistry()
    }

    s"return $Unauthorized when authentication fails" in new TestCase {
      routes(givenAuthAsUnauthorized)
        .call(request)
        .status shouldBe Unauthorized
    }

    s"return $NotFound when no auth header" in new TestCase {
      routes(maybeAuthUser = MaybeAuthUser.noUser).call(request).status shouldBe NotFound
    }

    s"return $NotFound when the user has no rights to the project" in new TestCase {

      val authUser = MaybeAuthUser(authUsers.generateOne)

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, authUser.option)
        .returning(leftT[IO, AuthContext[model.projects.Slug]](AuthorizationFailure))

      val response = routes(authUser).call(request)

      response.status        shouldBe NotFound
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error.unsafeApply(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name" should {

    s"return $Ok for valid path parameters" in new TestCase {

      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val projectSlug   = projectSlugs.generateOne

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(
          rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser.option, projectSlug))
        )

      val request = Request[IO](GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectSlug"))
      (projectDetailsEndpoint
        .`GET /projects/:slug`(_: model.projects.Slug, _: Option[AuthUser])(_: Request[IO]))
        .expects(projectSlug, maybeAuthUser.option, request)
        .returning(Response[IO](Ok).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok

      routesMetrics.clearRegistry()
    }

    s"return $NotFound for invalid project paths" in new TestCase {

      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val namespace     = nonBlankStrings().generateOne.value

      val response = routes(maybeAuthUser).call(
        Request(GET, uri"/knowledge-graph/projects" / namespace)
      )

      response.status        shouldBe NotFound
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Info("Resource not found")
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthAsUnauthorized)
        .call(Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectSlugs.generateOne}")))
        .status shouldBe Unauthorized
    }

    s"return $NotFound when auth user has no rights for the project" in new TestCase {

      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val projectSlug   = projectSlugs.generateOne

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(leftT[IO, AuthContext[model.projects.Slug]](AuthorizationFailure))

      val response = routes(maybeAuthUser).call(
        Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/$projectSlug"))
      )

      response.status        shouldBe NotFound
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error.unsafeApply(AuthorizationFailure.getMessage)
    }
  }

  "PATCH and PUT /knowledge-graph/projects/:namespace/../:name" should {

    val projectSlug = projectSlugs.generateOne

    Request[IO](PATCH, Uri.unsafeFromString(s"knowledge-graph/projects/$projectSlug")) ::
      Request[IO](PUT, Uri.unsafeFromString(s"knowledge-graph/projects/$projectSlug")) :: Nil foreach { request =>
        s"return $Accepted for valid path parameters, user and payload - case ${request.method}" in new TestCase {

          val authUser = MaybeAuthUser(authUsers.generateOne)

          (projectSlugAuthorizer.authorize _)
            .expects(projectSlug, authUser.option)
            .returning(
              rightT[IO, EndpointSecurityException](AuthContext(authUser.option, projectSlug))
            )

          (projectUpdateEndpoint
            .`PATCH /projects/:slug`(_: model.projects.Slug, _: Request[IO], _: AuthUser))
            .expects(projectSlug, request, authUser.option.get)
            .returning(Response[IO](Accepted).pure[IO])

          routes(authUser).call(request).status shouldBe Accepted

          routesMetrics.clearRegistry()
        }

        s"return $Unauthorized when authentication fails - case ${request.method}" in new TestCase {
          routes(givenAuthAsUnauthorized)
            .call(request)
            .status shouldBe Unauthorized
        }

        s"return $NotFound when no auth header - case ${request.method}" in new TestCase {
          routes(maybeAuthUser = MaybeAuthUser.noUser).call(request).status shouldBe NotFound
        }

        s"return $NotFound when the user has no rights to the project - case ${request.method}" in new TestCase {

          val authUser = MaybeAuthUser(authUsers.generateOne)

          (projectSlugAuthorizer.authorize _)
            .expects(projectSlug, authUser.option)
            .returning(leftT[IO, AuthContext[model.projects.Slug]](AuthorizationFailure))

          val response = routes(authUser).call(request)

          response.status        shouldBe NotFound
          response.contentType   shouldBe Some(`Content-Type`(application.json))
          response.body[Message] shouldBe Message.Error.unsafeApply(AuthorizationFailure.getMessage)
        }
      }
  }

  "GET /knowledge-graph/projects/:namespace/../:name/datasets" should {
    import projects.datasets.Endpoint.Criteria
    import projects.datasets.Endpoint.Criteria.Sort
    import projects.datasets.Endpoint.Criteria.Sort._

    val projectSlug = projectSlugs.generateOne
    val projectDsUri = projectSlug.toNamespaces
      .foldLeft(uri"/knowledge-graph/projects")(_ / _.show) / projectSlug.toPath / "datasets"
    val defaultPerPage = PerPage.max
    val defaultPaging  = PagingRequest.default.copy(perPage = defaultPerPage)

    forAll {
      Table(
        "uri"        -> "criteria",
        projectDsUri -> Criteria(projectSlug, paging = defaultPaging),
        sortingDirections
          .map(dir =>
            projectDsUri +? ("sort" -> s"name:$dir") -> Criteria(projectSlug,
                                                                 sorting = Sorting(Sort.By(ByName, dir)),
                                                                 paging = defaultPaging
            )
          )
          .generateOne,
        sortingDirections
          .map(dir =>
            projectDsUri +? ("sort" -> s"dateModified:$dir") -> Criteria(projectSlug,
                                                                         sorting =
                                                                           Sorting(Sort.By(ByDateModified, dir)),
                                                                         paging = defaultPaging
            )
          )
          .generateOne,
        pages
          .map(page =>
            projectDsUri +? ("page" -> page.show) -> Criteria(
              projectSlug,
              paging = PagingRequest.default.copy(page = page, perPage = defaultPerPage)
            )
          )
          .generateOne,
        perPages
          .map(perPage =>
            projectDsUri +? ("per_page" -> perPage.show) -> Criteria(projectSlug,
                                                                     paging =
                                                                       PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the parameters from $uri, pass them to the endpoint and return received response" in new TestCase {

        val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)

        (projectSlugAuthorizer.authorize _)
          .expects(criteria.projectSlug, maybeAuthUser.option)
          .returning(
            rightT[IO, EndpointSecurityException](
              AuthContext(maybeAuthUser.option, criteria.projectSlug)
            )
          )

        val request = Request[IO](GET, uri)

        (projectDatasetsEndpoint.`GET /projects/:slug/datasets` _)
          .expects(request, criteria)
          .returning(Response[IO](Ok).pure[IO])

        routes(maybeAuthUser).call(request).status shouldBe Ok

        routesMetrics.clearRegistry()
      }
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthAsUnauthorized)
        .call(
          Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/${projectSlugs.generateOne}/datasets"))
        )
        .status shouldBe Unauthorized
    }

    s"return $NotFound when auth user has no rights for the project" in new TestCase {

      val criteria      = projects.datasets.Endpoint.Criteria(projectSlugs.generateOne)
      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)

      (projectSlugAuthorizer.authorize _)
        .expects(criteria.projectSlug, maybeAuthUser.option)
        .returning(leftT[IO, AuthContext[model.projects.Slug]](AuthorizationFailure))

      val response = routes(maybeAuthUser)
        .call(Request(GET, Uri.unsafeFromString(s"knowledge-graph/projects/${criteria.projectSlug}/datasets")))

      response.status        shouldBe NotFound
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error.unsafeApply(AuthorizationFailure.getMessage)
    }
  }

  "GET /knowledge-graph/projects/:namespace/../:name/datasets/:dsSlug/tags" should {
    import projects.datasets.tags.Endpoint._

    val projectSlug = projectSlugs.generateOne
    val datasetSlug = datasetSlugs.generateOne
    val projectDsTagsUri = projectSlug.toNamespaces
      .foldLeft(uri"/knowledge-graph/projects")(_ / _) / projectSlug.toPath / "datasets" / datasetSlug / "tags"

    forAll {
      Table(
        "uri"            -> "criteria",
        projectDsTagsUri -> Criteria(projectSlug, datasetSlug),
        pages
          .map(page =>
            projectDsTagsUri +? ("page" -> page.show) -> Criteria(projectSlug,
                                                                  datasetSlug,
                                                                  PagingRequest.default.copy(page = page)
            )
          )
          .generateOne,
        perPages
          .map(perPage =>
            projectDsTagsUri +? ("per_page" -> perPage.show) -> Criteria(projectSlug,
                                                                         datasetSlug,
                                                                         PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the parameters from $uri, pass them to the endpoint and return received response" in new TestCase {
        val request = Request[IO](GET, uri)

        (projectSlugAuthorizer.authorize _)
          .expects(projectSlug, None)
          .returning(rightT[IO, EndpointSecurityException](AuthContext.forUnknownUser(projectSlug)))

        val responseBody = jsons.generateOne
        (projectDatasetTagsEndpoint
          .`GET /projects/:slug/datasets/:dsSlug/tags`(_: Criteria)(_: Request[IO]))
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {

      val invalidPerPage = nonEmptyStrings().generateOne

      val response = routes().call(Request[IO](GET, projectDsTagsUri +? ("per_page" -> invalidPerPage)))

      response.status        shouldBe BadRequest
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error.unsafeApply(s"'$invalidPerPage' not a valid 'per_page' value")
    }

    "authenticate user from the request if given" in new TestCase {

      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val request       = Request[IO](GET, projectDsTagsUri)

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(
          rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser.option, projectSlug))
        )

      val responseBody = jsons.generateOne
      (projectDatasetTagsEndpoint
        .`GET /projects/:slug/datasets/:dsSlug/tags`(_: Criteria)(_: Request[IO]))
        .expects(Criteria(projectSlug, datasetSlug, maybeUser = maybeAuthUser.option), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing()).call(Request[IO](GET, projectDsTagsUri)).status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/projects/:projectId/files/:location/lineage" should {
    def lineageUri(projectSlug: model.projects.Slug, location: Location) =
      Uri.unsafeFromString(s"knowledge-graph/projects/${projectSlug.show}/files/${urlEncode(location.show)}/lineage")

    s"return $Ok when the lineage is found" in new TestCase {
      val projectSlug   = projectSlugs.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val uri           = lineageUri(projectSlug, location)
      val request       = Request[IO](GET, uri)

      val responseBody = jsons.generateOne

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(
          rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser.option, projectSlug))
        )

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectSlug, location, maybeAuthUser.option)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      val response = routes(maybeAuthUser).call(request)

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      response.body[Json]  shouldBe responseBody
    }

    s"return $NotFound for a lineage which isn't found" in new TestCase {
      val projectSlug   = projectSlugs.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val uri           = lineageUri(projectSlug, location)

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(
          rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser.option, projectSlug))
        )

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectSlug, location, maybeAuthUser.option)
        .returning(Response[IO](NotFound).pure[IO])

      val response = routes(maybeAuthUser).call(Request[IO](GET, uri))

      response.status shouldBe NotFound
    }

    "authenticate user from the request if given" in new TestCase {

      val projectSlug   = projectSlugs.generateOne
      val location      = nodeLocations.generateOne
      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val request       = Request[IO](GET, lineageUri(projectSlug, location))

      val responseBody = jsons.generateOne

      (projectSlugAuthorizer.authorize _)
        .expects(projectSlug, maybeAuthUser.option)
        .returning(
          rightT[IO, EndpointSecurityException](AuthContext(maybeAuthUser.option, projectSlug))
        )

      (lineageEndpoint.`GET /lineage` _)
        .expects(projectSlug, location, maybeAuthUser.option)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, lineageUri(projectSlugs.generateOne, nodeLocations.generateOne)))
        .status shouldBe Unauthorized
    }
  }

  "GET /knowledge-graph/spec.json" should {

    s"return $Ok with OpenAPI json body" in new TestCase {
      val openApiJson = jsons.generateOne
      (() => docsEndpoint.`get /spec.json`)
        .expects()
        .returning(IO.pure(Response[IO](Ok).withEntity(openApiJson)))

      val response = routes().call(Request(GET, uri"/knowledge-graph/spec.json"))

      response.status     shouldBe Ok
      response.body[Json] shouldBe openApiJson
    }
  }

  "GET /knowledge-graph/users/:id/projects" should {
    import users.projects.Endpoint.Criteria._
    import users.projects.Endpoint._
    import users.projects._

    val userId = personGitLabIds.generateOne

    forAll {
      val commonUri = uri"/knowledge-graph/users" / userId.value / "projects"
      Table(
        "uri"     -> "criteria",
        commonUri -> Criteria(userId),
        activationStates
          .map(state => commonUri +? ("state" -> state.show) -> Criteria(userId, Filters(state), PagingRequest.default))
          .generateOne,
        pages
          .map(page =>
            commonUri +? ("page" -> page.show) -> Criteria(userId, paging = PagingRequest.default.copy(page = page))
          )
          .generateOne,
        perPages
          .map(perPage =>
            commonUri +? ("per_page" -> perPage.show) -> Criteria(userId,
                                                                  paging = PagingRequest.default.copy(perPage = perPage)
            )
          )
          .generateOne
      )
    } { (uri, criteria) =>
      s"read the query parameters from $uri, pass them to the endpoint and return received response" in new TestCase {
        val request = Request[IO](GET, uri)

        val responseBody = jsons.generateOne
        (usersProjectsEndpoint.`GET /users/:id/projects` _)
          .expects(criteria, request)
          .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

        val response = routes().call(request)

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))
        response.body[Json]  shouldBe responseBody
      }
    }

    s"return $BadRequest for invalid parameter values" in new TestCase {
      val response = routes()
        .call(
          Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects" +? ("state" -> -1))
        )

      response.status        shouldBe BadRequest
      response.contentType   shouldBe Some(`Content-Type`(application.json))
      response.body[Message] shouldBe Message.Error("'state' parameter with invalid value")
    }

    "authenticate user from the request if given" in new TestCase {

      val maybeAuthUser = MaybeAuthUser(authUsers.generateOption)
      val request       = Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects")

      val responseBody = jsons.generateOne
      (usersProjectsEndpoint.`GET /users/:id/projects` _)
        .expects(Criteria(userId, maybeUser = maybeAuthUser.option), request)
        .returning(Response[IO](Ok).withEntity(responseBody).pure[IO])

      routes(maybeAuthUser).call(request).status shouldBe Ok
    }

    s"return $Unauthorized when user authentication fails" in new TestCase {
      routes(givenAuthFailing())
        .call(Request[IO](GET, uri"/knowledge-graph/users" / userId.value / "projects"))
        .status shouldBe Unauthorized
    }
  }

  "GET /metrics" should {

    s"return $Ok with some prometheus metrics" in new TestCase {
      val response = routes().call(Request(GET, uri"/metrics"))

      response.status     shouldBe Ok
      response.body[String] should include("server_response_duration_seconds")
    }
  }

  "GET /ping" should {

    s"return $Ok with 'pong' body" in new TestCase {
      val response = routes().call(Request(GET, uri"/ping"))

      response.status       shouldBe Ok
      response.body[String] shouldBe "pong"
    }
  }

  "GET /version" should {
    "return response from the version endpoint" in new TestCase {
      routes().call(Request(GET, uri"/version")).status shouldBe versionEndpointResponse.status
    }
  }

  "GET /knowledge-graph/version" should {
    "return response from the version endpoint - the same as /version" in new TestCase {
      routes().call(Request(GET, uri"/knowledge-graph/version")).status shouldBe versionEndpointResponse.status
    }
  }

  private trait TestCase {

    private implicit val ru: RenkuUrl = renkuUrls.generateOne
    val datasetsSearchEndpoint          = mock[datasets.Endpoint[IO]]
    val datasetDetailsEndpoint          = mock[datasets.details.Endpoint[IO]]
    val entitiesEndpoint                = mock[entities.Endpoint[IO]]
    val recentEntitiesEndpoint          = mock[entities.currentuser.recentlyviewed.Endpoint[IO]]
    val lineageEndpoint                 = mock[projects.files.lineage.Endpoint[IO]]
    val ontologyEndpoint                = mock[ontology.Endpoint[IO]]
    val projectDeleteEndpoint           = mock[projects.delete.Endpoint[IO]]
    val projectDetailsEndpoint          = mock[projects.details.Endpoint[IO]]
    val projectCreateEndpoint           = mock[projects.create.Endpoint[IO]]
    val projectUpdateEndpoint           = mock[projects.update.Endpoint[IO]]
    val projectDatasetsEndpoint         = mock[projects.datasets.Endpoint[IO]]
    val projectDatasetTagsEndpoint      = mock[projects.datasets.tags.Endpoint[IO]]
    val docsEndpoint                    = mock[docs.Endpoint[IO]]
    val usersProjectsEndpoint           = mock[users.projects.Endpoint[IO]]
    val projectSlugAuthorizer           = mock[Authorizer[IO, model.projects.Slug]]
    private val datasetIdAuthorizer     = mock[Authorizer[IO, model.datasets.Identifier]]
    private val datasetSameAsAuthorizer = mock[Authorizer[IO, model.datasets.SameAs]]
    val routesMetrics                   = TestRoutesMetrics()
    private val versionRoutes           = mock[version.Routes[IO]]

    def routes(
        maybeAuthUser: MaybeAuthUser = MaybeAuthUser.noUser
    ): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] = routes(
      givenAuthIfNeededMiddleware(returning = IO.pure(maybeAuthUser))
    )

    def routes(middleware: AuthMiddleware[IO, MaybeAuthUser]): Resource[IO, Kleisli[IO, Request[IO], Response[IO]]] =
      new MicroserviceRoutes[IO](
        datasetsSearchEndpoint,
        datasetDetailsEndpoint,
        entitiesEndpoint,
        ontologyEndpoint,
        projectDeleteEndpoint,
        projectDetailsEndpoint,
        projectCreateEndpoint,
        projectUpdateEndpoint,
        projectDatasetsEndpoint,
        projectDatasetTagsEndpoint,
        recentEntitiesEndpoint,
        lineageEndpoint,
        docsEndpoint,
        usersProjectsEndpoint,
        middleware,
        projectSlugAuthorizer,
        datasetIdAuthorizer,
        datasetSameAsAuthorizer,
        routesMetrics,
        versionRoutes
      ).routes.map(_.or(notAvailableResponse))

    val versionEndpointResponse = Response[IO](httpStatuses.generateOne)
    (versionRoutes.apply _)
      .expects()
      .returning {
        import org.http4s.dsl.io.{GET => _, _}
        HttpRoutes.of[IO] { case GET -> Root / "version" => versionEndpointResponse.pure[IO] }
      }
      .atLeastOnce()

    {
      import org.http4s.dsl.io.{GET => _, _}
      (versionRoutes.on _)
        .expects(Root / "knowledge-graph" / "version")
        .returning {
          HttpRoutes.of[IO] { case GET -> Root / "knowledge-graph" / "version" => versionEndpointResponse.pure[IO] }
        }
        .atLeastOnce()
    }

    def givenDSAuthorizer(requestedDS:   RequestedDataset,
                          maybeAuthUser: Option[AuthUser],
                          returning:     EitherT[IO, EndpointSecurityException, AuthContext[RequestedDataset]]
    ) = requestedDS.fold(
      id => givenDSIdAuthorizer(id, maybeAuthUser, returning = returning.map(_.replaceKey(id))),
      sameAs => givenDSSameAsAuthorizer(sameAs, maybeAuthUser, returning = returning.map(_.replaceKey(sameAs)))
    )

    def givenDSIdAuthorizer(identifier:    model.datasets.Identifier,
                            maybeAuthUser: Option[AuthUser],
                            returning: EitherT[IO, EndpointSecurityException, AuthContext[model.datasets.Identifier]]
    ) = (datasetIdAuthorizer.authorize _)
      .expects(identifier, maybeAuthUser)
      .returning(returning)

    private def givenDSSameAsAuthorizer(
        sameAs:        model.datasets.SameAs,
        maybeAuthUser: Option[AuthUser],
        returning:     EitherT[IO, EndpointSecurityException, AuthContext[model.datasets.SameAs]]
    ) = (datasetSameAsAuthorizer.authorize _)
      .expects(sameAs, maybeAuthUser)
      .returning(returning)
  }
}
