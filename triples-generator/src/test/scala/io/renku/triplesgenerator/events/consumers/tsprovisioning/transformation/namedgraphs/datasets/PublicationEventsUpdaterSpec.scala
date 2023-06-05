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

package io.renku.triplesgenerator.events.consumers.tsprovisioning
package transformation.namedgraphs.datasets

import TransformationStep.Queries
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import transformation.Generators.queriesGen

import scala.util.Try

class PublicationEventsUpdaterSpec extends AnyFlatSpec with should.Matchers with TryValues with MockFactory {

  it should "leave the project unchanged and add delete queries for all datasets' publicationEvents" in {

    val initialProject = {
      val p = anyRenkuProjectEntities.generateOne
      p.addDatasets(datasetEntities(provenanceNonModified).generateList(p.dateCreated): _*)
      p.to[entities.Project]
    }

    val deletionQueries = initialProject.datasets >>= { ds =>
      val dsDeletionQueries = sparqlQueries.generateList()

      (updatesCreator.deletePublicationEvents _)
        .expects(initialProject.resourceId, ds)
        .returning(dsDeletionQueries)

      dsDeletionQueries
    }

    val initialQueries = queriesGen.generateOne

    val (updatedProject, updatedQueries) =
      updater.updatePublicationEvents(initialProject -> initialQueries).success.value

    updatedProject shouldBe initialProject
    updatedQueries shouldBe initialQueries ++ Queries.preDataQueriesOnly(deletionQueries)
  }

  private lazy val updatesCreator = mock[UpdatesCreator]
  private lazy val updater        = new PublicationEventsUpdaterImpl[Try](updatesCreator)
}
