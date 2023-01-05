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

package io.renku.entities.searchgraphs

import PersonInfo._
import cats.syntax.all._
import io.renku.entities.searchgraphs.SearchInfo.DateModified
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{Date, TopmostSameAs}
import io.renku.graph.model.entities
import io.renku.graph.model.testentities.Dataset.DatasetImagesOps
import io.renku.graph.model.testentities._
import org.scalacheck.Gen

private object Generators {

  lazy val searchInfoObjects: Gen[SearchInfo] = for {
    topmostSameAs     <- datasetTopmostSameAs
    name              <- datasetNames
    visibility        <- projectVisibilities
    date              <- datasetDates
    maybeDateModified <- modifiedDates(notYoungerThan = date).toGeneratorOfOptions
    creators          <- personEntities.map(_.to[entities.Person]).map(toPersonInfo).toGeneratorOfNonEmptyList(max = 2)
    keywords          <- datasetKeywords.toGeneratorOfList(max = 2)
    maybeDesc         <- datasetDescriptions.toGeneratorOfOptions
    images            <- imageUris.toGeneratorOfList(max = 2).map(_.toEntitiesImages(datasetResourceIds.generateOne))
    links             <- linkObjects(topmostSameAs).toGeneratorOfNonEmptyList(max = 2)
  } yield SearchInfo(topmostSameAs,
                     name,
                     visibility,
                     date,
                     maybeDateModified,
                     creators,
                     keywords,
                     maybeDesc,
                     images,
                     links
  )

  def modifiedDates(notYoungerThan: Date): Gen[DateModified] =
    timestampsNotInTheFuture(notYoungerThan.instant).generateAs(DateModified(_))

  def linkObjects(topmostSameAs: TopmostSameAs): Gen[Link] =
    (datasetResourceIds, projectResourceIds, projectPaths)
      .mapN(Link(topmostSameAs, _, _, _))
}
