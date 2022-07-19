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

package io.renku.graph.model

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.datasets._
import io.renku.graph.model.persons.{Affiliation, Email, Name, Username}
import io.renku.graph.model.projects.{FilePath, Id, Path, ResourceId, Visibility}
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, const, frequency, numChar, oneOf}

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

object GraphModelGenerators {

  implicit val renkuUrls: Gen[RenkuUrl] =
    httpUrls(hostGenerator = nonEmptyStrings().map(_.toLowerCase)) map RenkuUrl.apply

  implicit val gitLabUrls: Gen[GitLabUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 2)
  } yield GitLabUrl(s"$url/$path")

  implicit val gitLabApiUrls: Gen[GitLabApiUrl] = gitLabUrls.map(_.apiV4)

  implicit val cliVersions: Gen[CliVersion] = for {
    version       <- semanticVersions
    commitsNumber <- positiveInts(999)
    commitPart <- shas.toGeneratorOfOptions.map(_.map(_.take(7)).map(sha => s".dev$commitsNumber+g$sha").getOrElse(""))
  } yield CliVersion(s"$version$commitPart")

  implicit val usernames: Gen[persons.Username] = nonBlankStrings(minLength = 5).map(v => Username(v.value))
  implicit val personAffiliations: Gen[persons.Affiliation] =
    nonBlankStrings(minLength = 4).map(v => Affiliation(v.value))

  implicit val personEmails: Gen[persons.Email] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf("!$%&*+-/=?_~".toList))
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

  implicit val personNames: Gen[persons.Name] = for {
    first <- nonBlankStrings(
               minLength = 3,
               charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '`'))
             )
    second <- nonBlankStrings(
                minLength = 3,
                charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '`'))
              )
  } yield Name(s"$first $second")

  def personGitLabResourceId(implicit renkuUrl: RenkuUrl): Gen[persons.ResourceId.GitLabIdBased] =
    personGitLabIds map persons.ResourceId.apply
  def personOrcidResourceId(implicit renkuUrl: RenkuUrl): Gen[persons.ResourceId.OrcidIdBased] =
    personOrcidIds map persons.ResourceId.apply
  val personEmailResourceId: Gen[persons.ResourceId.EmailBased] = personEmails map persons.ResourceId.apply
  def personNameResourceId(implicit renkuUrl: RenkuUrl): Gen[persons.ResourceId.NameBased] =
    personNames map persons.ResourceId.apply
  implicit def personResourceIds(implicit renkuUrl: RenkuUrl): Gen[persons.ResourceId] =
    Gen.oneOf(personGitLabResourceId, personOrcidResourceId, personEmailResourceId, personNameResourceId)

  implicit lazy val personGitLabIds: Gen[persons.GitLabId] =
    Gen.uuid.map(_ => persons.GitLabId(Random.nextInt(100000000) + 1))
  implicit lazy val personOrcidIds: Gen[persons.OrcidId] =
    Gen
      .choose(1000, 9999)
      .toGeneratorOfList(minElements = 4, maxElements = 4)
      .map(_.mkString("https://orcid.org/", "-", ""))
      .toGeneratorOf(persons.OrcidId)

  implicit val projectIds:   Gen[Id]            = Gen.uuid.map(_ => Id(Random.nextInt(1000000) + 1))
  implicit val projectNames: Gen[projects.Name] = nonBlankStrings(minLength = 5) map (n => projects.Name(n.value))
  implicit val projectDescriptions: Gen[projects.Description] = paragraphs() map (v => projects.Description(v.value))
  implicit val projectVisibilities: Gen[Visibility]           = Gen.oneOf(Visibility.all.toList)
  def projectCreatedDates(min: Instant = Instant.EPOCH): Gen[projects.DateCreated] =
    timestamps(min, max = Instant.now()).toGeneratorOf(projects.DateCreated)

  implicit val projectNamespaces: Gen[projects.Namespace] = {
    val firstCharGen    = frequency(6 -> alphaChar, 2 -> numChar, 1 -> const('_'))
    val nonFirstCharGen = frequency(6 -> alphaChar, 2 -> numChar, 1 -> oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- nonEmptyList(nonFirstCharGen, minElements = 5, maxElements = 10)
    } yield projects.Namespace(s"$firstChar${otherChars.toList.mkString("")}")
  }

  implicit val projectPaths: Gen[Path] = relativePaths(
    minSegments = 2,
    maxSegments = 5,
    partsGenerator = projectNamespaces.map(_.show)
  ) map Path.apply

  implicit val projectResourceIds: Gen[ResourceId] = projectResourceIds()(renkuUrls.generateOne)
  def projectResourceIds()(implicit renkuUrl: RenkuUrl): Gen[ResourceId] =
    projectPaths map (path => ResourceId.from(s"$renkuUrl/projects/$path").fold(throw _, identity))

  implicit val projectKeywords: Gen[projects.Keyword] =
    nonBlankStrings(minLength = 5).map(v => projects.Keyword(v.value))

  implicit val projectSchemaVersions: Gen[SchemaVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(SchemaVersion.apply)

  implicit val filePaths: Gen[FilePath] = relativePaths() map FilePath.apply

  implicit val datasetIdentifiers: Gen[Identifier] = noDashUuid.toGeneratorOf(Identifier)

  implicit val datasetOriginalIdentifiers: Gen[OriginalIdentifier] =
    datasetIdentifiers map (id => OriginalIdentifier(id.toString))
  implicit val datasetTitles:       Gen[datasets.Title] = nonEmptyStrings() map datasets.Title.apply
  implicit val datasetNames:        Gen[datasets.Name]  = nonEmptyStrings() map datasets.Name.apply
  implicit val datasetDescriptions: Gen[Description]    = paragraphs() map (_.value) map Description.apply
  implicit val datasetImageUris:    Gen[ImageUri]       = Gen.oneOf(relativePaths(), httpUrls()) map ImageUri.apply
  implicit val datasetExternalSameAs: Gen[ExternalSameAs] =
    validatedUrls map SameAs.external map (_.fold(throw _, identity))
  def datasetInternalSameAsFrom(renkuUrlGen:  Gen[RenkuUrl] = renkuUrls,
                                datasetIdGen: Gen[Identifier] = datasetIdentifiers
  ): Gen[InternalSameAs] = for {
    url <- renkuUrlGen
    id  <- datasetIdGen
  } yield SameAs.internal(url / "datasets" / id).fold(throw _, identity)
  implicit val datasetInternalSameAs: Gen[InternalSameAs] = datasetInternalSameAsFrom()
  implicit val datasetSameAs:         Gen[SameAs]         = Gen.oneOf(datasetExternalSameAs, datasetInternalSameAs)
  implicit val datasetTopmostSameAs:  Gen[TopmostSameAs]  = datasetSameAs.map(TopmostSameAs.apply)
  implicit val datasetDerivedFroms:   Gen[DerivedFrom]    = validatedUrls map (_.value) map DerivedFrom.apply
  implicit val datasetTopmostDerivedFroms: Gen[TopmostDerivedFrom] = datasetDerivedFroms.map(TopmostDerivedFrom.apply)
  implicit val datasetResourceIds: Gen[datasets.ResourceId] =
    datasetIdentifiers.map(id => datasets.ResourceId((renkuUrls.generateOne / "datasets" / id).show))

  def datasetPublishedDates(min: DatePublished = LocalDate.EPOCH): Gen[DatePublished] =
    timestamps(min.value.atStartOfDay().toInstant(ZoneOffset.UTC), max = Instant.now())
      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
      .map(DatePublished)

  def datasetCreatedDates(min: Instant = Instant.EPOCH): Gen[DateCreated] =
    timestamps(min, max = Instant.now()) map DateCreated.apply

  lazy val datasetDates: Gen[Date] =
    Gen.oneOf(datasetCreatedDates(), datasetPublishedDates(DatePublished(LocalDate.EPOCH)))

  implicit val datasetKeywords: Gen[datasets.Keyword] = nonBlankStrings(minLength = 5) map (_.value) map Keyword.apply
  implicit val datasetLicenses: Gen[datasets.License] = httpUrls() map License.apply
  implicit val datasetVersions: Gen[datasets.Version] = semanticVersions map Version.apply

  implicit val datasetPartIds:       Gen[PartId]       = noDashUuid.toGeneratorOf(PartId)
  implicit val datasetPartExternals: Gen[PartExternal] = booleans map PartExternal.apply
  implicit val datasetPartSources:   Gen[PartSource]   = httpUrls() map PartSource.apply
  implicit val datasetPartLocations: Gen[PartLocation] =
    relativePaths(minSegments = 2, maxSegments = 2)
      .map(path => s"data/$path")
      .map(PartLocation.apply)
}
