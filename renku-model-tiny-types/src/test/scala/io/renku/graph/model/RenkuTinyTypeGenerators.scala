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

package io.renku.graph.model

import cats.syntax.all._
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.images.{ImageResourceId, ImageUri}
import io.renku.graph.model.versions.{CliVersion, SchemaVersion}
import io.renku.tinytypes.InstantTinyType
import org.scalacheck.Gen

import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, ZoneOffset}
import scala.util.Random

trait RenkuTinyTypeGenerators {

  def associationResourceIdGen: Gen[associations.ResourceId] =
    Generators.validatedUrls.map(_.value).map(associations.ResourceId)

  def invalidationTimes(min: InstantTinyType): Gen[InvalidationTime] = invalidationTimes(min.value)

  def invalidationTimes(min: Instant*): Gen[InvalidationTime] =
    Generators.timestamps(min = min.max, max = Instant.now()).toGeneratorOf(InvalidationTime)

  implicit val cliVersions: Gen[CliVersion] = for {
    version       <- Generators.semanticVersions
    commitsNumber <- Generators.positiveInts(999)
    commitPart <-
      Generators.shas.toGeneratorOfOptions.map(_.map(_.take(7)).map(sha => s".dev$commitsNumber+g$sha").getOrElse(""))
  } yield CliVersion(s"$version$commitPart")

  implicit val projectSchemaVersions: Gen[SchemaVersion] = Gen
    .listOfN(3, Generators.positiveInts(max = 50))
    .map(_.mkString("."))
    .map(SchemaVersion.apply)

  implicit val renkuUrls: Gen[RenkuUrl] =
    Generators.httpUrls(hostGenerator = Generators.nonEmptyStrings().map(_.toLowerCase)) map RenkuUrl.apply

  implicit val personAffiliations: Gen[persons.Affiliation] =
    Generators.nonBlankStrings(minLength = 4).map(v => persons.Affiliation(v.value))

  implicit val personEmails: Gen[persons.Email] = {
    val firstCharGen    = Gen.frequency(15 -> Gen.alphaChar, 4 -> Gen.numChar, 1 -> Gen.oneOf("!$&*+-/=?_~".toList))
    val nonFirstCharGen = Gen.frequency(13 -> Gen.alphaChar, 6 -> Gen.numChar, 1 -> Gen.oneOf("!#$&*+-/=?_~.".toList))
    val beforeAts = for {
      firstChar  <- firstCharGen
      otherChars <- Generators.nonEmptyList(nonFirstCharGen, min = 5, max = 10)
    } yield s"$firstChar${otherChars.toList.mkString("")}"

    for {
      beforeAt <- beforeAts
      afterAt  <- Generators.nonEmptyStrings()
    } yield persons.Email(s"$beforeAt@$afterAt")
  }

  implicit val personUsernames: Gen[persons.Username] =
    Generators.nonBlankStrings(minLength = 5).map(v => persons.Username(v.value))
  implicit val personNames: Gen[persons.Name] = for {
    first <- Generators.nonBlankStrings(
               minLength = 3,
               charsGenerator = Gen.frequency(9 -> Gen.alphaChar, 1 -> Gen.oneOf('-', '_', '\\', '`'))
             )
    second <- Generators.nonBlankStrings(
                minLength = 3,
                charsGenerator = Gen.frequency(9 -> Gen.alphaChar, 1 -> Gen.oneOf('-', '_', '\\', '`'))
              )
  } yield persons.Name(s"$first $second")

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
      .toGeneratorOfList(min = 4, max = 4)
      .map(_.mkString("https://orcid.org/", "-", ""))
      .toGeneratorOf(persons.OrcidId)

  implicit val projectIds: Gen[projects.GitLabId] = Gen.uuid.map(_ => projects.GitLabId(Random.nextInt(1000000) + 1))
  implicit val projectNames: Gen[projects.Name] =
    Generators.nonBlankStrings(minLength = 5) map (n => projects.Name(n.value))
  implicit val projectDescriptions: Gen[projects.Description] =
    Generators.paragraphs() map (v => projects.Description(v.value))
  implicit val projectVisibilities: Gen[projects.Visibility] = Gen.oneOf(projects.Visibility.all.toList)

  def projectCreatedDates(min: Instant = Instant.EPOCH): Gen[projects.DateCreated] =
    Generators.timestamps(min, max = Instant.now()).toGeneratorOf(projects.DateCreated)

  implicit val projectNamespaces: Gen[projects.Namespace] = {
    val firstCharGen    = Gen.frequency(6 -> Gen.alphaChar, 2 -> Gen.numChar, 1 -> Gen.const('_'))
    val nonFirstCharGen = Gen.frequency(6 -> Gen.alphaChar, 2 -> Gen.numChar, 1 -> Gen.oneOf('_', '.', '-'))
    for {
      firstChar  <- firstCharGen
      otherChars <- Generators.nonEmptyList(nonFirstCharGen, min = 5, max = 10)
    } yield projects.Namespace(s"$firstChar${otherChars.toList.mkString("")}")
  }

  implicit val projectPaths: Gen[projects.Path] = Generators.relativePaths(
    minSegments = 2,
    maxSegments = 5,
    partsGenerator = projectNamespaces.map(_.show)
  ) map projects.Path.apply

  implicit val projectResourceIds: Gen[projects.ResourceId] = projectResourceIds()(renkuUrls.generateOne)

  def projectResourceIds()(implicit renkuUrl: RenkuUrl): Gen[projects.ResourceId] =
    projectPaths map (path => projects.ResourceId.from(s"$renkuUrl/projects/$path").fold(throw _, identity))

  def projectImageResourceIds(project: projects.ResourceId, max: Int = 5): Gen[List[ImageResourceId]] =
    Gen
      .chooseNum(0, max)
      .map(n => (0 until n).map(i => ImageResourceId((project / "images" / i.toString).value)).toList)

  implicit val projectKeywords: Gen[projects.Keyword] =
    Generators.nonBlankStrings(minLength = 5).map(v => projects.Keyword(v.value))

  implicit val filePaths: Gen[projects.FilePath] = Generators.relativePaths() map projects.FilePath.apply

  implicit val datasetIdentifiers: Gen[datasets.Identifier] = Generators.noDashUuid.toGeneratorOf(datasets.Identifier)
  implicit val datasetResourceIds: Gen[datasets.ResourceId] = datasetResourceIds()(renkuUrls.generateOne)
  def datasetResourceIds(datasetIdGen: Gen[datasets.Identifier] = datasetIdentifiers)(implicit
      renkuUrl: RenkuUrl
  ): Gen[datasets.ResourceId] = datasetIdGen.map(id => datasets.ResourceId((renkuUrl / "datasets" / id).show))

  implicit val datasetOriginalIdentifiers: Gen[datasets.OriginalIdentifier] =
    datasetIdentifiers map (id => datasets.OriginalIdentifier(id.toString))
  implicit val datasetTitles: Gen[datasets.Title] = Generators.nonEmptyStrings(minLength = 4) map datasets.Title.apply
  implicit val datasetNames:  Gen[datasets.Name]  = Generators.nonEmptyStrings(minLength = 4) map datasets.Name.apply
  implicit val datasetDescriptions: Gen[datasets.Description] =
    Generators.paragraphs() map (_.value) map datasets.Description.apply
  implicit val imageUris: Gen[ImageUri] =
    Gen.oneOf(Generators.relativePaths(), Generators.httpUrls()) map ImageUri.apply
  implicit val datasetExternalSameAs: Gen[datasets.ExternalSameAs] =
    Generators.validatedUrls.map(datasets.SameAs.external).map(_.fold(throw _, identity))

  def datasetInternalSameAsFrom(renkuUrlGen:  Gen[RenkuUrl] = renkuUrls,
                                datasetIdGen: Gen[datasets.Identifier] = datasetIdentifiers
  ): Gen[datasets.InternalSameAs] = for {
    url <- renkuUrlGen
    id  <- datasetIdGen
  } yield datasets.SameAs.internal(url / "datasets" / id).fold(throw _, identity)

  implicit val datasetInternalSameAs: Gen[datasets.InternalSameAs] = datasetInternalSameAsFrom()
  implicit val datasetSameAs: Gen[datasets.SameAs] = Gen.oneOf(datasetExternalSameAs, datasetInternalSameAs)
  implicit val datasetTopmostSameAs: Gen[datasets.TopmostSameAs] = datasetSameAs.map(datasets.TopmostSameAs.apply)
  implicit val datasetDerivedFroms: Gen[datasets.DerivedFrom] =
    Generators.validatedUrls.map(_.value).map(datasets.DerivedFrom.apply)
  implicit val datasetTopmostDerivedFroms: Gen[datasets.TopmostDerivedFrom] =
    datasetDerivedFroms.map(datasets.TopmostDerivedFrom.apply)

  def datasetPublishedDates(min: datasets.DatePublished = LocalDate.EPOCH): Gen[datasets.DatePublished] =
    Generators
      .timestamps(min.value.atStartOfDay().toInstant(ZoneOffset.UTC), max = Instant.now())
      .map(LocalDate.ofInstant(_, ZoneOffset.UTC))
      .map(datasets.DatePublished)

  def datasetCreatedDates(min: Instant = Instant.EPOCH): Gen[datasets.DateCreated] =
    Generators.timestamps(min, max = Instant.now()).map(datasets.DateCreated.apply)

  lazy val datasetCreatedOrPublished: Gen[datasets.CreatedOrPublished] =
    Gen.oneOf(datasetCreatedDates(), datasetPublishedDates(datasets.DatePublished(LocalDate.EPOCH)))

  def datasetModifiedDates(notYoungerThan: datasets.CreatedOrPublished): Gen[datasets.DateModified] =
    timestampsNotInTheFuture(notYoungerThan.instant).generateAs(datasets.DateModified(_))

  implicit val datasetKeywords: Gen[datasets.Keyword] =
    Generators.nonBlankStrings(minLength = 5) map (_.value) map datasets.Keyword.apply
  implicit val datasetLicenses: Gen[datasets.License] = Generators.httpUrls() map datasets.License.apply
  implicit val datasetVersions: Gen[datasets.Version] = Generators.semanticVersions map datasets.Version.apply

  implicit val datasetPartIds:       Gen[datasets.PartId]       = Generators.noDashUuid.toGeneratorOf(datasets.PartId)
  implicit val datasetPartExternals: Gen[datasets.PartExternal] = Generators.booleans map datasets.PartExternal.apply
  implicit val datasetPartSources:   Gen[datasets.PartSource]   = Generators.httpUrls() map datasets.PartSource.apply
  implicit val datasetPartLocations: Gen[datasets.PartLocation] =
    Generators
      .relativePaths(minSegments = 2, maxSegments = 2)
      .map(path => s"data/$path")
      .map(datasets.PartLocation.apply)

  implicit val publicationEventNames: Gen[publicationEvents.Name] =
    nonEmptyStrings(minLength = 5).toGeneratorOf(publicationEvents.Name.apply)
  def publicationEventResourceIds(nameGen:      Gen[publicationEvents.Name] = publicationEventNames,
                                  datasetIdGen: Gen[datasets.ResourceId] = datasetResourceIds
  )(implicit renkuUrl: RenkuUrl): Gen[publicationEvents.ResourceId] =
    (nameGen -> datasetIdGen).mapN((name, dsId) =>
      publicationEvents.ResourceId((renkuUrl / "datasettags" / show"$name@$dsId").show)
    )
  def publicationEventAbout(datasetIdentifierGen: Gen[datasets.Identifier] = datasetIdentifiers)(implicit
      renkuUrl: RenkuUrl
  ): Gen[publicationEvents.About] =
    datasetIdentifierGen.map(datasetIdentifier => (renkuUrl / "urls" / "datasets" / datasetIdentifier).show)

  implicit val publicationEventDesc: Gen[publicationEvents.Description] =
    sentences().map(_.value).map(publicationEvents.Description.apply)

  implicit val planIdentifiers: Gen[plans.Identifier] = Generators.noDashUuid.toGeneratorOf(plans.Identifier)

  def planResourceIds(implicit renkuUrl: RenkuUrl): Gen[plans.ResourceId] = planIdentifiers.map(plans.ResourceId(_))

  implicit val planNames: Gen[plans.Name] =
    Generators.nonBlankStrings(minLength = 5).map(_.value).toGeneratorOf[plans.Name]
  implicit val planKeywords: Gen[plans.Keyword] =
    Generators.nonBlankStrings(minLength = 5) map (_.value) map plans.Keyword.apply
  implicit val planDescriptions: Gen[plans.Description] =
    Generators.sentences().map(_.value).toGeneratorOf[plans.Description]
  implicit val planCommands: Gen[plans.Command] = Generators.nonBlankStrings().map(_.value).toGeneratorOf[plans.Command]
  implicit val planProgrammingLanguages: Gen[plans.ProgrammingLanguage] =
    Generators.nonBlankStrings().map(_.value).toGeneratorOf[plans.ProgrammingLanguage]
  implicit val planSuccessCodes: Gen[plans.SuccessCode] =
    Generators.positiveInts().map(_.value).toGeneratorOf[plans.SuccessCode]
  implicit val planDerivedFroms: Gen[plans.DerivedFrom] =
    Generators.validatedUrls.map(_.value).map(plans.DerivedFrom.apply)

  def planCreatedDates(after: InstantTinyType): Gen[plans.DateCreated] =
    Generators.timestampsNotInTheFuture(after.value).toGeneratorOf(plans.DateCreated)
  def planModifiedDates(after: InstantTinyType): Gen[plans.DateModified] =
    Generators.timestampsNotInTheFuture(after.value).toGeneratorOf(plans.DateModified)

  val entityResourceIds: Gen[entityModel.ResourceId] =
    Generators.validatedUrls.map(_.value).map(entityModel.ResourceId)
  val entityFileLocations: Gen[entityModel.Location.File] =
    Generators.relativePaths() map entityModel.Location.File.apply
  val entityFolderLocations: Gen[entityModel.Location.Folder] =
    Generators.relativePaths() map entityModel.Location.Folder.apply
  val entityLocations: Gen[entityModel.Location] = Gen.oneOf(entityFileLocations, entityFolderLocations)
  val entityChecksums: Gen[entityModel.Checksum] =
    Generators.nonBlankStrings(40, 40).map(_.value).map(entityModel.Checksum.apply)

  val activityResourceIdGen: Gen[activities.ResourceId] =
    Generators.validatedUrls.map(_.value).map(activities.ResourceId)

  val activityStartTimes: Gen[activities.StartTime] =
    Generators.timestampsNotInTheFuture.map(activities.StartTime.apply)
  val activityEndTimeGen: Gen[activities.EndTime] =
    Generators.timestampsNotInTheFuture.map(activities.EndTime.apply)

  def activityStartTimes(after: InstantTinyType): Gen[activities.StartTime] =
    Generators.timestampsNotInTheFuture(after.value).toGeneratorOf(activities.StartTime)

  implicit val commandParameterNames: Gen[commandParameters.Name] =
    Generators.nonBlankStrings().map(_.value).toGeneratorOf[commandParameters.Name]

  implicit val commandParameterEncodingFormats: Gen[commandParameters.EncodingFormat] =
    Gen
      .oneOf(
        StandardCharsets.UTF_8.name(),
        StandardCharsets.US_ASCII.name(),
        StandardCharsets.ISO_8859_1.name,
        StandardCharsets.UTF_16.name(),
        Generators.nonEmptyStrings().generateOne
      )
      .map(commandParameters.EncodingFormat(_))

  implicit val commandParameterFolderCreation: Gen[commandParameters.FolderCreation] =
    Gen.oneOf(commandParameters.FolderCreation.yes, commandParameters.FolderCreation.no)

  val commandParameterDescription: Gen[commandParameters.Description] =
    Generators.sentences().map(s => commandParameters.Description(s.value))

  val commandParameterResourceId: Gen[commandParameters.ResourceId] =
    Generators.validatedUrls.map(_.value).map(commandParameters.ResourceId)

  val commandParameterPositionGen: Gen[commandParameters.Position] =
    Generators.positiveInts(max = 5).map(_.value).map(commandParameters.Position.apply)

  val commandParameterDefaultValueGen: Gen[commandParameters.ParameterDefaultValue] =
    Generators.nonBlankStrings().map(v => commandParameters.ParameterDefaultValue(v.value))

  val commandParameterPrefixGen: Gen[commandParameters.Prefix] =
    Generators.nonBlankStrings(maxLength = 2).map(s => commandParameters.Prefix(s"-$s"))

  val generationsResourceIdGen: Gen[generations.ResourceId] =
    Generators.validatedUrls.map(_.value).map(generations.ResourceId)

  val partResourceIdGen: Gen[datasets.PartResourceId] =
    Generators.validatedUrls.map(id => datasets.PartResourceId(id.value))

  val agentResourceIdGen: Gen[agents.ResourceId] =
    Generators.validatedUrls.map(_.value).map(agents.ResourceId)

  val agentNameGen: Gen[agents.Name] =
    Generators.nonBlankStrings().map(_.value).map(agents.Name)

  val parameterValueIdGen: Gen[parameterValues.ResourceId] =
    Generators.validatedUrls.map(_.value).map(parameterValues.ResourceId)

  val parameterLinkResourceIdGen: Gen[parameterLinks.ResourceId] =
    Generators.validatedUrls.map(_.value).map(parameterLinks.ResourceId)

  val usageResourceIdGen: Gen[usages.ResourceId] =
    Generators.validatedUrls.map(_.value).map(usages.ResourceId)
}

object RenkuTinyTypeGenerators extends RenkuTinyTypeGenerators
