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

package io.renku.graph.model

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.datasets._
import io.renku.graph.model.projects.{FilePath, Id, Path, ResourceId, Visibility}
import io.renku.graph.model.users.{Affiliation, Email, Name, Username}
import org.scalacheck.Gen
import org.scalacheck.Gen.{alphaChar, const, frequency, numChar, oneOf, uuid}

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import scala.util.Random

object GraphModelGenerators {

  implicit val renkuBaseUrls: Gen[RenkuBaseUrl] = httpUrls() map RenkuBaseUrl.apply

  implicit val gitLabUrls: Gen[GitLabUrl] = for {
    url  <- httpUrls()
    path <- relativePaths(maxSegments = 2)
  } yield GitLabUrl(s"$url/$path")

  implicit val gitLabApiUrls: Gen[GitLabApiUrl] = gitLabUrls.map(_.apiV4)

  implicit val cliVersions: Gen[CliVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(CliVersion.apply)

  implicit val usernames:        Gen[users.Username]    = nonBlankStrings(minLength = 5).map(v => Username(v.value))
  implicit val userAffiliations: Gen[users.Affiliation] = nonBlankStrings(minLength = 4).map(v => Affiliation(v.value))

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
    first <- nonBlankStrings(
               minLength = 3,
               charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '/', '`'))
             )
    second <- nonBlankStrings(
                minLength = 3,
                charsGenerator = frequency(9 -> alphaChar, 1 -> oneOf('-', '_', '\\', '/', '`'))
              )
  } yield Name(s"$first $second")

  implicit val userResourceIds: Gen[users.ResourceId] = userResourceIds(userEmails.toGeneratorOfOptions)
  def userResourceIds(maybeEmail: Option[Email]): Gen[users.ResourceId] = userResourceIds(Gen.const(maybeEmail))
  def userResourceIds(maybeEmailGen: Gen[Option[Email]]): Gen[users.ResourceId] = for {
    maybeEmail <- maybeEmailGen
  } yield users.ResourceId
    .from(maybeEmail.map(email => s"mailto:$email").getOrElse(s"_:${UUID.randomUUID()}"))
    .fold(throw _, identity)

  implicit val userGitLabIds: Gen[users.GitLabId] = Gen.uuid.map(_ => users.GitLabId(Random.nextInt(100000000) + 1))

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

  implicit val datasetIdentifiers: Gen[Identifier] = uuid
    .map(_.toString)
    .map(Identifier.apply)

  implicit val datasetInitialVersions: Gen[InitialVersion] = datasetIdentifiers map (id => InitialVersion(id.toString))
  implicit val datasetTitles:          Gen[datasets.Title] = nonEmptyStrings() map datasets.Title.apply
  implicit val datasetNames:           Gen[datasets.Name]  = nonEmptyStrings() map datasets.Name.apply
  implicit val datasetDescriptions:    Gen[Description]    = paragraphs() map (_.value) map Description.apply
  implicit val datasetImageUris:       Gen[ImageUri]       = Gen.oneOf(relativePaths(), httpUrls()) map ImageUri.apply
  implicit val datasetExternalSameAs: Gen[ExternalSameAs] =
    validatedUrls map SameAs.external map (_.fold(throw _, identity))
  implicit val datasetInternalSameAs: Gen[InternalSameAs] = for {
    url <- renkuBaseUrls
    id  <- datasetIdentifiers
  } yield SameAs.internal(url / "datasets" / id).fold(throw _, identity)
  implicit val datasetSameAs:        Gen[SameAs]        = Gen.oneOf(datasetExternalSameAs, datasetInternalSameAs)
  implicit val datasetTopmostSameAs: Gen[TopmostSameAs] = datasetSameAs.map(TopmostSameAs.apply)
  implicit val datasetDerivedFroms:  Gen[DerivedFrom]   = validatedUrls map (_.value) map DerivedFrom.apply
  implicit val datasetTopmostDerivedFroms: Gen[TopmostDerivedFrom] = datasetDerivedFroms.map(TopmostDerivedFrom.apply)
  implicit val datasetResourceIds: Gen[datasets.ResourceId] =
    datasetIdentifiers.map(id => datasets.ResourceId((renkuBaseUrls.generateOne / "datasets" / id).show))

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
