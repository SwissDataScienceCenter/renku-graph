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

package io.renku.knowledgegraph.docs

import cats.effect.IO
import io.circe.Json
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.http.server.EndpointTester._
import io.renku.knowledgegraph.datasets.{DatasetEndpointDocs, DatasetSearchEndpointDocs, ProjectDatasetsEndpointDocs}
import io.renku.knowledgegraph.{ontology, projectdetails}
import io.renku.knowledgegraph.docs.OpenApiTester._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model.{Path, Uri}
import io.renku.knowledgegraph.entities
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "GET /spec.json" should {

    "return the OpenAPI specification" in new TestCase {
      validateDocument {
        endpoint.`get /spec.json`.unsafeRunSync().as[Json].unsafeRunSync()
      }
    }
  }

  private trait TestCase {

    private val datasetsSearchEndpoint = mock[DatasetSearchEndpointDocs]
    (() => datasetsSearchEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "datasets")))
    private val datasetEndpoint = mock[DatasetEndpointDocs]
    (() => datasetEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "datasets" / "1234")))
    private val entitiesEndpoint = mock[entities.EndpointDocs]
    (() => entitiesEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "entities")))
    private val ontologyEndpoint = mock[ontology.EndpointDocs]
    (() => ontologyEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "ontology")))
    private val projectEndpoint = mock[projectdetails.EndpointDocs]
    (() => projectEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "projects" / "namespace" / "name")))
    private val projectDatasetsEndpoint = mock[ProjectDatasetsEndpointDocs]
    (() => projectDatasetsEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "projects" / "datasets")))
    private val docsEndpoint = mock[EndpointDocs]
    (() => docsEndpoint.path)
      .expects()
      .returns(Path(nonEmptyStrings().generateOne, description = None, GET(Uri / "spec.json")))

    val endpoint = new EndpointImpl[IO](datasetsSearchEndpoint,
                                        datasetEndpoint,
                                        entitiesEndpoint,
                                        ontologyEndpoint,
                                        projectEndpoint,
                                        projectDatasetsEndpoint,
                                        docsEndpoint,
                                        serviceVersions.generateOne
    )
  }
}
