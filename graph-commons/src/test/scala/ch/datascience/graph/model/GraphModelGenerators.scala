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

package ch.datascience.graph.model

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects.{FilePath, Id, Path, ResourceId, Visibility}
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, choose, const, frequency, numChar, oneOf, uuid}

object GraphModelGenerators {

  implicit val projectIds: Gen[Id] = for {
    min <- choose(1, 1000)
    max <- choose(1001, 100000)
    id  <- choose(min, max)
  } yield Id(id)
  implicit val projectNames:        Gen[projects.Name]        = nonEmptyStrings() map projects.Name.apply
  implicit val projectDescriptions: Gen[projects.Description] = paragraphs() map (v => projects.Description(v.value))
  implicit val projectVisibilities: Gen[Visibility]           = Gen.oneOf(Visibility.all.toList)
  implicit val projectCreatedDates: Gen[projects.DateCreated] = timestampsNotInTheFuture map projects.DateCreated.apply
  implicit val projectPaths: Gen[Path] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    val partsGenerator = for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"

    relativePaths(
      minSegments    = 2,
      maxSegments    = 5,
      partsGenerator = partsGenerator
    ) map Path.apply
  }
  implicit val projectResources: Gen[ResourceId] = for {
    url  <- httpUrls()
    path <- projectPaths
  } yield ResourceId.from(s"$url/projects/$path").fold(throw _, identity)
  implicit val filePaths: Gen[FilePath] = relativePaths() map FilePath.apply

  implicit val datasetIdentifiers: Gen[Identifier] = Gen
    .oneOf(
      uuid.map(_.toString),
      for {
        first  <- Gen.choose(10, 99)
        second <- Gen.choose(1000, 9999)
        third  <- Gen.choose(1000000, 9999999)
      } yield s"$first.$second/zenodo.$third"
    )
    .map(Identifier.apply)
  implicit val datasetNames:          Gen[Name]          = nonEmptyStrings() map Name.apply
  implicit val datasetDescriptions:   Gen[Description]   = paragraphs() map (_.value) map Description.apply
  implicit val datasetUrls:           Gen[Url]           = validatedUrls map (_.value) map Url.apply
  implicit val datasetSameAs:         Gen[SameAs]        = validatedUrls map (_.value) map SameAs.apply
  implicit val datasetPublishedDates: Gen[PublishedDate] = localDatesNotInTheFuture map PublishedDate.apply
  implicit val datasetCreatedDates:   Gen[DateCreated]   = timestampsNotInTheFuture map DateCreated.apply
  implicit val datasetKeywords:       Gen[Keyword]       = nonBlankStrings() map (_.value) map Keyword.apply
  implicit val datasetPartNames:      Gen[PartName]      = nonEmptyStrings() map PartName.apply
  implicit val datasetPartLocations: Gen[PartLocation] =
    relativePaths(minSegments = 2, maxSegments = 2)
      .map(path => s"data/$path")
      .map(PartLocation.apply)
  implicit val datasetInProjectCreationDates: Gen[DateCreatedInProject] =
    timestampsNotInTheFuture map DateCreatedInProject.apply
}
