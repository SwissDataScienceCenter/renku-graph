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

package io.renku.entities.searchgraphs.datasets
package commands

import Generators._
import SearchInfoLens.{linkProjectId, linkVisibility}
import TSDatasetSearchInfo._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities, projects}
import org.scalacheck.Gen

private object CalculatorInfoSetGenerators {

  implicit lazy val tsDatasetSearchInfoObjects: Gen[TSDatasetSearchInfo] = for {
    topmostSameAs <- datasetTopmostSameAs
    links         <- linkObjectsGen(topmostSameAs).toGeneratorOfList(max = 2)
  } yield TSDatasetSearchInfo(topmostSameAs, links)

  def tsDatasetSearchInfoObjects(withLinkTo: projects.ResourceId, and: projects.ResourceId*): Gen[TSDatasetSearchInfo] =
    tsDatasetSearchInfoObjects.map { i =>
      tsSearchInfoLinks.replace {
        List.from(withLinkTo :: and.toList).map(linkProjectId.replace(_)(linkObjectsGen(i.topmostSameAs).generateOne))
      }(i)
    }

  def tsDatasetSearchInfoObjects(withLinkTo:   entities.Project,
                                 topSameAsGen: Gen[datasets.TopmostSameAs] = datasetTopmostSameAs
  ): Gen[TSDatasetSearchInfo] =
    tsDatasetSearchInfoObjects.map(_.copy(topmostSameAs = topSameAsGen.generateOne)).flatMap { si =>
      linkObjectsGen(si.topmostSameAs)
        .map(linkProjectId replace withLinkTo.resourceId)
        .map(linkVisibility replace withLinkTo.visibility)
        .map(link => tsSearchInfoLinks.replace(List(link))(si))
    }

  lazy val calculatorInfoSets: Gen[CalculatorInfoSet] = for {
    project   <- anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project])
    modelInfo <- modelDatasetSearchInfoObjects(project)
    tsInfo    <- tsDatasetSearchInfoObjects(project)
  } yield CalculatorInfoSet.BothInfos(project.identification, modelInfo, tsInfo)
}
