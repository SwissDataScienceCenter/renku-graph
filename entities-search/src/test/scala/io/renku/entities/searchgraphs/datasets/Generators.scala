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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.PersonInfo._
import io.renku.entities.searchgraphs.datasets.SearchInfoLens._
import io.renku.entities.searchgraphs.datasets.commands.UpdateCommand
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.testentities.Dataset.DatasetImagesOps
import io.renku.graph.model.testentities._
import io.renku.graph.model.{datasets, entities, projects}
import io.renku.triplesstore.client.TriplesStoreGenerators.quads
import org.scalacheck.Gen

private object Generators {

  implicit lazy val searchInfoObjectsGen: Gen[SearchInfo] = for {
    topmostSameAs      <- datasetTopmostSameAs
    name               <- datasetNames
    createdOrPublished <- datasetCreatedOrPublished
    visibility         <- projectVisibilities
    maybeDateModified  <- datasetModifiedDates(notYoungerThan = createdOrPublished).toGeneratorOfOptions
    creators           <- personInfos.toGeneratorOfNonEmptyList(max = 2)
    keywords           <- datasetKeywords.toGeneratorOfList(max = 2)
    maybeDesc          <- datasetDescriptions.toGeneratorOfOptions
    images             <- imageUris.toGeneratorOfList(max = 2).map(_.toEntitiesImages(datasetResourceIds.generateOne))
    links              <- linkObjectsGen(topmostSameAs).toGeneratorOfNonEmptyList(max = 2)
  } yield SearchInfo(topmostSameAs,
                     name,
                     visibility,
                     createdOrPublished,
                     maybeDateModified,
                     creators,
                     keywords,
                     maybeDesc,
                     images.toList,
                     links
  )

  def searchInfoObjectsGen(withLinkTo: entities.Project): Gen[SearchInfo] =
    searchInfoObjectsGen(withLinkTo.resourceId).map(_.copy(visibility = withLinkTo.visibility))

  def searchInfoObjectsGen(withLinkTo: projects.ResourceId, and: projects.ResourceId*): Gen[SearchInfo] =
    searchInfoObjectsGen.map { i =>
      searchInfoLinks
        .modify { _ =>
          val linkedProjects = NonEmptyList.of(withLinkTo, and: _*)
          linkedProjects.map(linkProjectId.set(_)(linkObjectsGen(i.topmostSameAs).generateOne))
        }(i)
    }

  lazy val personInfos: Gen[PersonInfo] =
    personEntities.map(_.to[entities.Person]).map(toPersonInfo)

  def linkObjectsGen(topmostSameAs: TopmostSameAs,
                     projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = Gen.oneOf(originalDatasetLinkObjectsGen(topmostSameAs, projectIdGen),
                           importedDatasetLinkObjectsGen(topmostSameAs, projectIdGen)
  )

  def originalDatasetLinkObjectsGen(topmostSameAs: TopmostSameAs,
                                    projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = (projectIdGen, projectPaths)
    .mapN(Link(topmostSameAs, datasets.ResourceId(topmostSameAs.value), _, _))

  def importedDatasetLinkObjectsGen(topmostSameAs: TopmostSameAs,
                                    projectIdGen:  Gen[projects.ResourceId] = projectResourceIds
  ): Gen[Link] = (datasetResourceIds, projectIdGen, projectPaths)
    .mapN(Link(topmostSameAs, _, _, _))

  val updateCommands: Gen[UpdateCommand] =
    quads.flatMap(quad => Gen.oneOf(UpdateCommand.Insert(quad), UpdateCommand.Delete(quad)))
}
