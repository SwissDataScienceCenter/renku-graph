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

import java.util.UUID

import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects.{FilePath, Id, Path, ResourceId, Visibility}
import ch.datascience.graph.model.users.{Affiliation, Email, Name, Username}
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, choose, const, frequency, numChar, oneOf, uuid}

object GraphModelGenerators {

  implicit val usernames:        Gen[users.Username]    = nonEmptyStrings() map Username.apply
  implicit val userAffiliations: Gen[users.Affiliation] = nonEmptyStrings() map Affiliation.apply

  implicit val userEmails: Gen[users.Email] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf("!#$%&*+-/=?_~".toList))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf("!#$%&*+-/=?_~.".toList))
    val beforeAts = for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"

    for {
      beforeAt <- beforeAts
      afterAt  <- nonEmptyStrings()
    } yield Email(s"$beforeAt@$afterAt")
  }

  implicit val userNames: Gen[users.Name] = for {
    first  <- nonEmptyStrings()
    second <- nonEmptyStrings()
  } yield Name(s"$first $second")

  implicit val userResourceIds: Gen[users.ResourceId] = userResourceIds(userEmails.toGeneratorOfOptions)
  def userResourceIds(maybeEmail:    Option[Email]): Gen[users.ResourceId] = userResourceIds(Gen.const(maybeEmail))
  def userResourceIds(maybeEmailGen: Gen[Option[Email]]): Gen[users.ResourceId] =
    for {
      maybeEmail <- maybeEmailGen
    } yield users.ResourceId
      .from(maybeEmail.map(email => s"mailto:$email").getOrElse(s"_:${UUID.randomUUID()}"))
      .fold(throw _, identity)

  implicit val projectIds: Gen[Id] = for {
    min <- choose(1, 1000)
    max <- choose(1001, 100000)
    id  <- choose(min, max)
  } yield Id(id)
  implicit val projectNames:        Gen[projects.Name]        = nonBlankStrings(minLength = 5) map (n => projects.Name(n.value))
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
  implicit val projectResourceIds: Gen[ResourceId] = projectResourceIds()(renkuBaseUrls.generateOne)
  def projectResourceIds()(implicit renkuBaseUrl: RenkuBaseUrl): Gen[ResourceId] =
    for {
      path <- projectPaths
    } yield ResourceId.from(s"$renkuBaseUrl/projects/$path").fold(throw _, identity)
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
  implicit val datasetNames:          Gen[datasets.Name] = nonEmptyStrings() map datasets.Name.apply
  implicit val datasetDescriptions:   Gen[Description]   = paragraphs() map (_.value) map Description.apply
  implicit val datasetUrls:           Gen[Url]           = validatedUrls map (_.value) map Url.apply
  val datasetUrlSameAs:               Gen[UrlSameAs]     = validatedUrls map (_.value) map SameAs.fromUrl map (_.fold(throw _, identity))
  val datasetIdSameAs:                Gen[IdSameAs]      = validatedUrls map (_.value) map SameAs.fromId map (_.fold(throw _, identity))
  implicit val datasetSameAs:         Gen[SameAs]        = Gen.oneOf(datasetUrlSameAs, datasetIdSameAs)
  implicit val datasetDerivedFroms:   Gen[DerivedFrom]   = validatedUrls map (_.value) map DerivedFrom.apply
  implicit val datasetPublishedDates: Gen[PublishedDate] = localDatesNotInTheFuture map PublishedDate.apply
  implicit val datasetCreatedDates:   Gen[DateCreated]   = timestampsNotInTheFuture map DateCreated.apply
  implicit val datasetKeywords:       Gen[Keyword]       = nonBlankStrings() map (_.value) map Keyword.apply
  implicit val datasetPartNames:      Gen[PartName]      = nonBlankStrings(minLength = 5) map (v => PartName(v.value))
  implicit val datasetPartLocations: Gen[PartLocation] =
    relativePaths(minSegments = 2, maxSegments = 2)
      .map(path => s"data/$path")
      .map(PartLocation.apply)
  implicit val datasetInProjectCreationDates: Gen[DateCreatedInProject] =
    timestampsNotInTheFuture map DateCreatedInProject.apply
}
