/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.CommonGraphGenerators.{accessTokens, renkuBaseUrls}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects.{FilePath, Id, Path, ResourceId, Visibility}
import ch.datascience.graph.model.users.{Affiliation, Email, Name, Username}
import ch.datascience.http.server.security.model.AuthUser
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, choose, const, frequency, numChar, oneOf, uuid}

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

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
    first  <- nonEmptyStrings(charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '/', '`')))
    second <- nonEmptyStrings(charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '/', '`')))
  } yield Name(s"$first $second")

  implicit val userResourceIds: Gen[users.ResourceId] = userResourceIds(userEmails.toGeneratorOfOptions)
  def userResourceIds(maybeEmail:    Option[Email]): Gen[users.ResourceId] = userResourceIds(Gen.const(maybeEmail))
  def userResourceIds(maybeEmailGen: Gen[Option[Email]]): Gen[users.ResourceId] =
    for {
      maybeEmail <- maybeEmailGen
    } yield users.ResourceId
      .from(maybeEmail.map(email => s"mailto:$email").getOrElse(s"_:${UUID.randomUUID()}"))
      .fold(throw _, identity)

  implicit val userGitLabIds: Gen[users.GitLabId] = nonNegativeInts().map(users.GitLabId(_))

  implicit val authUsers: Gen[AuthUser] = for {
    gitLabId    <- userGitLabIds
    accessToken <- accessTokens
  } yield AuthUser(gitLabId, accessToken)

  implicit val projectIds: Gen[Id] = for {
    min <- choose(1, 1000)
    max <- choose(1001, 100000)
    id  <- choose(min, max)
  } yield Id(id)
  implicit val projectNames:        Gen[projects.Name]        = nonBlankStrings(minLength = 5) map (n => projects.Name(n.value))
  implicit val projectDescriptions: Gen[projects.Description] = paragraphs() map (v => projects.Description(v.value))
  implicit val projectVisibilities: Gen[Visibility]           = Gen.oneOf(Visibility.all.toList)
  def projectCreatedDates(min: Instant = Instant.EPOCH): Gen[projects.DateCreated] =
    timestamps(min, max = Instant.now()) map projects.DateCreated.apply
  implicit val projectPaths: Gen[Path] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    val partsGenerator = for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"

    relativePaths(
      minSegments = 2,
      maxSegments = 5,
      partsGenerator = partsGenerator
    ) map Path.apply
  }
  implicit val projectResourceIds: Gen[ResourceId] = projectResourceIds()(renkuBaseUrls.generateOne)
  def projectResourceIds()(implicit renkuBaseUrl: RenkuBaseUrl): Gen[ResourceId] =
    for {
      path <- projectPaths
    } yield ResourceId.from(s"$renkuBaseUrl/projects/$path").fold(throw _, identity)

  implicit val projectSchemaVersions: Gen[SchemaVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(SchemaVersion.apply)

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
  implicit val datasetInitialVersions: Gen[InitialVersion] = datasetIdentifiers map (id => InitialVersion(id.toString))
  implicit val datasetTitles:          Gen[datasets.Title] = nonEmptyStrings() map datasets.Title.apply
  implicit val datasetNames:           Gen[datasets.Name]  = nonEmptyStrings() map datasets.Name.apply
  implicit val datasetDescriptions:    Gen[Description]    = paragraphs() map (_.value) map Description.apply
  implicit val datasetUrls:            Gen[Url]            = validatedUrls map (_.value) map Url.apply
  implicit val datasetImageUris:       Gen[ImageUri]       = relativePaths() map ImageUri.apply
  implicit val datasetExternalSameAs: Gen[ExternalSameAs] =
    validatedUrls map SameAs.external map (_.fold(throw _, identity))
  implicit val datasetInternalSameAs: Gen[InternalSameAs] = for {
    url <- renkuBaseUrls
    id  <- datasetIdentifiers
  } yield SameAs.internal(url / "datasets" / id).fold(throw _, identity)
  implicit val datasetSameAs:              Gen[SameAs]             = Gen.oneOf(datasetExternalSameAs, datasetInternalSameAs)
  implicit val datasetTopmostSameAs:       Gen[TopmostSameAs]      = datasetSameAs.map(TopmostSameAs.apply)
  implicit val datasetDerivedFroms:        Gen[DerivedFrom]        = validatedUrls map (_.value) map DerivedFrom.apply
  implicit val datasetTopmostDerivedFroms: Gen[TopmostDerivedFrom] = datasetDerivedFroms.map(TopmostDerivedFrom.apply)

  def datasetPublishedDates(min: DatePublished = LocalDate.EPOCH): Gen[DatePublished] =
    timestamps(min.value.atStartOfDay().toInstant(ZoneOffset.UTC), max = Instant.now())
      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
      .map(DatePublished)

  def datasetCreatedDates(min: Instant = Instant.EPOCH): Gen[DateCreated] =
    timestamps(min, max = Instant.now()) map DateCreated.apply

  lazy val datasetDates: Gen[Date] =
    Gen.oneOf(datasetCreatedDates(), datasetPublishedDates(DatePublished(LocalDate.EPOCH)))

  implicit val datasetKeywords: Gen[Keyword] = nonBlankStrings(minLength = 5) map (_.value) map Keyword.apply
  implicit val datasetLicenses: Gen[License] = httpUrls() map License.apply
  implicit val datasetVersions: Gen[Version] = semanticVersions map Version.apply

  implicit val datasetPartExternals: Gen[PartExternal] = booleans map PartExternal.apply
  implicit val datasetPartSources:   Gen[PartSource]   = httpUrls() map PartSource.apply
  implicit val datasetPartLocations: Gen[PartLocation] =
    relativePaths(minSegments = 2, maxSegments = 2)
      .map(path => s"data/$path")
      .map(PartLocation.apply)
}
