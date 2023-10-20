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

import SearchInfoLens._
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities.Dataset.DatasetImagesOps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities, projects}
import org.scalacheck.Gen

private object Generators {

  implicit lazy val datasetSearchInfoObjects: Gen[DatasetSearchInfo] = for {
    topmostSameAs      <- datasetTopmostSameAs
    name               <- datasetNames
    slug               <- datasetSlugs
    createdOrPublished <- datasetCreatedOrPublished
    maybeDateModified  <- datasetModifiedDates(notYoungerThan = createdOrPublished).toGeneratorOfOptions
    creators           <- creators.toGeneratorOfNonEmptyList(max = 2)
    keywords           <- datasetKeywords.toGeneratorOfList(max = 2)
    maybeDesc          <- datasetDescriptions.toGeneratorOfOptions
    images             <- imageUris.toGeneratorOfList(max = 2).map(_.toEntitiesImages(datasetResourceIds.generateOne))
    links              <- linkObjectsGen(topmostSameAs).toGeneratorOfNonEmptyList()
  } yield DatasetSearchInfo(topmostSameAs,
                            name,
                            slug,
                            createdOrPublished,
                            maybeDateModified,
                            creators,
                            keywords,
                            maybeDesc,
                            images.toList,
                            links
  )

  def datasetSearchInfoObjects(project: entities.Project): Gen[DatasetSearchInfo] =
    datasetSearchInfoObjects
      .map { si =>
        val link = linkObjectsGen(si.topmostSameAs).generateOne
        searchInfoLinks.replace(NonEmptyList.one(updateLinkProject(project)(link)))(si)
      }

  implicit lazy val modelDatasetSearchInfoObjects: Gen[ModelDatasetSearchInfo] = for {
    topmostSameAs      <- datasetTopmostSameAs
    name               <- datasetNames
    slug               <- datasetSlugs
    createdOrPublished <- datasetCreatedOrPublished
    maybeDateModified  <- datasetModifiedDates(notYoungerThan = createdOrPublished).toGeneratorOfOptions
    creators           <- creators.toGeneratorOfNonEmptyList(max = 2)
    keywords           <- datasetKeywords.toGeneratorOfList(max = 2)
    maybeDesc          <- datasetDescriptions.toGeneratorOfOptions
    images             <- imageUris.toGeneratorOfList(max = 2).map(_.toEntitiesImages(datasetResourceIds.generateOne))
    link               <- linkObjectsGen(topmostSameAs)
  } yield ModelDatasetSearchInfo(topmostSameAs,
                                 name,
                                 slug,
                                 createdOrPublished,
                                 maybeDateModified,
                                 creators,
                                 keywords,
                                 maybeDesc,
                                 images.toList,
                                 link
  )

  def modelDatasetSearchInfoObjects(withLinkTo: entities.Project): Gen[ModelDatasetSearchInfo] =
    modelDatasetSearchInfoObjects
      .map(modelSearchInfoLink andThen linkProjectId replace withLinkTo.resourceId)
      .map(modelSearchInfoLink andThen linkVisibility replace withLinkTo.visibility)

  lazy val creators: Gen[Creator] =
    (personResourceIds, personNames).mapN(Creator.apply)

  def linkObjectsGen(topmostSameAs: TopmostSameAs,
                     projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = Gen.oneOf(originalDatasetLinkObjectsGen(topmostSameAs, projectIdGen),
                           importedDatasetLinkObjectsGen(topmostSameAs, projectIdGen)
  )

  def originalDatasetLinkObjectsGen(topmostSameAs: TopmostSameAs,
                                    projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = (projectIdGen, projectSlugs, projectVisibilities)
    .mapN(Link.from(topmostSameAs, datasets.ResourceId(topmostSameAs.value), _, _, _))

  def importedDatasetLinkObjectsGen(topmostSameAs: TopmostSameAs,
                                    projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = (datasetResourceIds, projectIdGen, projectSlugs, projectVisibilities)
    .mapN(Link.from(topmostSameAs, _, _, _, _))
}
