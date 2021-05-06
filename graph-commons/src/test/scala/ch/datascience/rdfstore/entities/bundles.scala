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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{cliVersions, gitLabUrls}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.Schemas
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model._
import ch.datascience.graph.model.datasets.{Dates, DerivedFrom, Description, Identifier, ImageUri, Keyword, Name, PartLocation, PartName, SameAs, Title, TopmostDerivedFrom, TopmostSameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, Path, Visibility}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.CommandParameter.Mapping.IOStream
import ch.datascience.rdfstore.entities.CommandParameter.PositionInfo.Position
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.DataSetPart.{DataSetPartArtifact, dataSetParts}
import ch.datascience.rdfstore.entities.ProcessRun.StandAloneProcessRun
import ch.datascience.rdfstore.entities.RunPlan.{Command, ProcessRunPlan}
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalacheck.Gen

import java.time.temporal.ChronoUnit.DAYS
import java.time.{Duration, Instant}

object bundles extends Schemas {

  implicit lazy val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")
  implicit lazy val gitLabApiUrl: GitLabApiUrl = gitLabUrls.generateOne.apiV4

  def generateAgent: Agent = Agent(cliVersions.generateOne)

  def generateProject(path: Path): Project = Project(
    path,
    projectNames.generateOne,
    projectCreatedDates.generateOne,
    projectCreators.generateOption,
    projectVisibilities.generateOption,
    version = projectSchemaVersions.generateOne
  )

  def fileCommit(
      location:      Location = locations.generateOne,
      commitId:      CommitId = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
      cliVersion:    CliVersion = cliVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeVisibility:     Option[Visibility] = None,
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeParent:         Option[Project] = None,
      projectVersion:      SchemaVersion
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD = Activity(
    commitId,
    committedDate,
    committer,
    Project(projectPath,
            projectName,
            projectDateCreated,
            maybeProjectCreator,
            maybeVisibility,
            maybeParentProject = maybeParent,
            version = projectVersion
    ),
    Agent(cliVersion),
    maybeGenerationFactories = List(
      Generation.factory(Entity.factory(location))
    )
  ).asJsonLD

  def randomDataSetCommit(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    randomDataSetActivity.asJsonLD

  def randomDataSetActivity: Activity =
    Gen.oneOf(nonModifiedDataSetActivity()()(), modifiedDataSetActivity()()()).generateOne

  def nonModifiedDataSetCommit(
      commitId:      CommitId = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
      cliVersion:    CliVersion = cliVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      maybeVisibility:     Option[Visibility] = None,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeParent:         Option[Project] = None,
      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
  )(
      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
      datasetTitle:               Title = datasetTitles.generateOne,
      datasetName:                Name = datasetNames.generateOne,
      datasetUrl:                 Url = datasetUrls.generateOne,
      maybeDatasetSameAs:         Option[SameAs] = Gen.option(datasetSameAs).generateOne,
      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
      dates:                      Dates = datasetDates.generateOne,
      datasetCreators:            Set[Person] = setOf(persons).generateOne,
      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
      datasetImages:              List[ImageUri] = listOf(imageUris).generateOne,
      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  )(implicit renkuBaseUrl:        RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    nonModifiedDataSetActivity(
      commitId,
      committedDate,
      committer,
      cliVersion
    )(projectPath, projectName, maybeVisibility, projectDateCreated, maybeProjectCreator, maybeParent, projectVersion)(
      datasetIdentifier,
      datasetTitle,
      datasetName,
      datasetUrl,
      maybeDatasetSameAs,
      maybeDatasetDescription,
      dates,
      datasetCreators,
      datasetParts,
      datasetKeywords,
      datasetImages,
      overrideTopmostSameAs,
      overrideTopmostDerivedFrom
    ).asJsonLD

  def nonModifiedDataSetActivity(
      commitId:      CommitId = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
      cliVersion:    CliVersion = cliVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      maybeVisibility:     Option[Visibility] = None,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeParent:         Option[Project] = None,
      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
  )(
      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
      datasetTitle:               Title = datasetTitles.generateOne,
      datasetName:                Name = datasetNames.generateOne,
      datasetUrl:                 Url = datasetUrls.generateOne,
      maybeDatasetSameAs:         Option[SameAs] = Gen.option(datasetSameAs).generateOne,
      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
      dates:                      Dates = datasetDates.generateOne,
      datasetCreators:            Set[Person] = setOf(persons).generateOne,
      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
      datasetImages:              List[ImageUri] = listOf(imageUris).generateOne,
      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  ): Activity = Activity(
    commitId,
    committedDate,
    committer,
    Project(projectPath,
            projectName,
            projectDateCreated,
            maybeProjectCreator,
            maybeVisibility,
            maybeParentProject = maybeParent,
            version = projectVersion
    ),
    Agent(cliVersion),
    maybeGenerationFactories = List(
      Generation.factory(
        DataSet.nonModifiedFactory(
          datasetIdentifier,
          datasetTitle,
          datasetName,
          datasetUrl,
          maybeDatasetSameAs,
          maybeDatasetDescription,
          dates,
          datasetCreators,
          datasetParts.map { case (name, location) =>
            DataSetPart.factory(name, location, None)(_)
          },
          datasetKeywords,
          datasetImages,
          overrideTopmostSameAs = overrideTopmostSameAs,
          overrideTopmostDerivedFrom = overrideTopmostDerivedFrom
        )
      )
    )
  )

  def modifiedDataSetCommit(
      commitId:      CommitId = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
      cliVersion:    CliVersion = cliVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeVisibility:     Option[Visibility] = None,
      maybeParent:         Option[Project] = None,
      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
  )(
      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
      datasetTitle:               Title = datasetTitles.generateOne,
      datasetName:                Name = datasetNames.generateOne,
      datasetUrl:                 Url = datasetUrls.generateOne,
      datasetDerivedFrom:         DerivedFrom = datasetDerivedFroms.generateOne,
      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
      dates:                      Dates = datasetDates.generateOne,
      datasetCreators:            Set[Person] = setOf(persons).generateOne,
      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
      datasetImages:              List[ImageUri] = listOf(imageUris).generateOne,
      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  )(implicit renkuBaseUrl:        RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    modifiedDataSetActivity(
      commitId,
      committedDate,
      committer,
      cliVersion
    )(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeVisibility, maybeParent, projectVersion)(
      datasetIdentifier,
      datasetTitle,
      datasetName,
      datasetUrl,
      datasetDerivedFrom,
      maybeDatasetDescription,
      dates,
      datasetCreators,
      datasetParts,
      datasetKeywords,
      datasetImages,
      overrideTopmostSameAs,
      overrideTopmostDerivedFrom
    ).asJsonLD

  def modifiedDataSetActivity(
      commitId:      CommitId = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
      cliVersion:    CliVersion = cliVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeVisibility:     Option[Visibility] = None,
      maybeParent:         Option[Project] = None,
      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
  )(
      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
      datasetTitle:               Title = datasetTitles.generateOne,
      datasetName:                Name = datasetNames.generateOne,
      datasetUrl:                 Url = datasetUrls.generateOne,
      datasetDerivedFrom:         DerivedFrom = datasetDerivedFroms.generateOne,
      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
      dates:                      Dates = datasetDates.generateOne,
      datasetCreators:            Set[Person] = setOf(persons).generateOne,
      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
      datasetImages:              List[ImageUri] = listOf(imageUris).generateOne,
      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
  ): Activity = Activity(
    commitId,
    committedDate,
    committer,
    Project(projectPath,
            projectName,
            projectDateCreated,
            maybeProjectCreator,
            maybeVisibility,
            maybeParentProject = maybeParent,
            version = projectVersion
    ),
    Agent(cliVersion),
    maybeGenerationFactories = List(
      Generation.factory(
        DataSet.modifiedFactory(
          datasetIdentifier,
          datasetTitle,
          datasetName,
          datasetUrl,
          datasetDerivedFrom,
          maybeDatasetDescription,
          dates,
          datasetCreators,
          datasetParts.map { case (name, location) =>
            DataSetPart.factory(name, location, None)(_)
          },
          datasetKeywords,
          datasetImages,
          overrideTopmostSameAs = overrideTopmostSameAs,
          overrideTopmostDerivedFrom = overrideTopmostDerivedFrom
        )
      )
    )
  )

  object exemplarLineageFlow {

    final case class ExemplarData(
        location:              Location,
        commitId:              CommitId,
        committer:             Person,
        `sha3 zhbikes`:        NodeDef,
        `sha7 plot_data`:      NodeDef,
        `sha7 clean_data`:     NodeDef,
        `sha8 renku run`:      NodeDef,
        `sha8 renku run date`: Instant,
        `sha8 parquet`:        NodeDef,
        `sha9 renku run`:      NodeDef,
        `sha9 renku run date`: Instant,
        `sha9 grid_plot`:      NodeDef,
        `sha9 cumulative`:     NodeDef,
        `sha10 zhbikes`:       NodeDef,
        `sha12 parquet`:       NodeDef,
        `sha12 parquet date`:  Instant
    )

    def apply(
        projectPath:         Path = projectPaths.generateOne,
        cliVersion:          CliVersion = cliVersions.generateOne,
        projectVisibility:   Option[Visibility] = projectVisibilities.generateSome,
        projectMembers:      Set[Person] = Set.empty[Person]
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): (List[JsonLD], ExemplarData) = {
      val project = Project(
        projectPath,
        projectNames.generateOne,
        projectCreatedDates.generateOne,
        projectCreators.generateOption,
        projectVisibility,
        members = projectMembers,
        version = projectSchemaVersions.generateOne
      )
      val agent           = Agent(cliVersion)
      val dataSetId       = datasets.Identifier("d67a1653-0b6e-463b-89a0-afe72a53c8bb")
      val dataSetCreators = nonEmptySet(persons).generateOne

      val dataSetFolder    = Location("data/zhbikes")
      val plotData         = Location("src/plot_data.py")
      val cleanData        = Location("src/clean_data.py")
      val bikesParquet     = Location("data/preprocessed/zhbikes.parquet")
      val cumulativePng    = Location("figs/cumulative.png")
      val gridPlotPng      = Location("figs/grid_plot.png")
      val velo2018Location = Location("data/zhbikes/2018velo.csv")
      val velo2019Location = Location("data/zhbikes/2019velo.csv")

      def dataSetGenerationFactory(partsFactories: List[Activity => DataSetPartArtifact]) =
        Generation.factory(
          entityFactory = DataSet.nonModifiedFactory(
            id = dataSetId,
            title = datasets.Title("zhbikes"),
            name = datasets.Name("zhbikes"),
            url = datasetUrls.generateOne,
            dates = datasetDates.generateOne,
            creators = dataSetCreators,
            partsFactories = partsFactories
          )
        )

      val commit2DataSetCreation = Activity(
        CommitId("000002"),
        relativeTimestamps(moreThanAgo = Duration.ofDays(30)).generateAs(CommittedDate),
        persons.generateOne,
        project,
        agent,
        comment = "renku dataset create zhbikes",
        maybeGenerationFactories = List(dataSetGenerationFactory(partsFactories = Nil))
      )

      val commit3AddingDataSetFile = Activity(
        CommitId("000003"),
        CommittedDate(commit2DataSetCreation.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit2DataSetCreation),
        maybeGenerationFactories = List(
          Generation.factory(
            entityFactory = Collection.factory(dataSetFolder, List(velo2019Location))
          )
        )
      )

      val commit4Activity = Activity(
        CommitId("000004"),
        CommittedDate(commit3AddingDataSetFile.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku dataset add zhbikes",
        maybeGenerationFactories = List(
          dataSetGenerationFactory(
            partsFactories = List(
              DataSetPart.factory(
                datasets.PartName("2018velo.csv"),
                datasets.PartLocation(velo2018Location.value),
                datasetUrls.generateOption
              )
            )
          )
        )
      )

      val commit5Activity = Activity(
        CommitId("000005"),
        CommittedDate(commit4Activity.committedDate.value.plus(1, DAYS)),
        committer = persons.generateOne,
        project,
        agent,
        comment = "packages installed",
        maybeInformedBy = commit4Activity.some,
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(Location("requirements.txt")))
        )
      )

      val commit6Activity = Activity(
        CommitId("000006"),
        CommittedDate(commit5Activity.committedDate.value.plus(1, DAYS)),
        committer = persons.generateOne,
        project,
        agent,
        comment = "added notebook",
        maybeInformedBy = commit5Activity.some,
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(Location("notebooks/zhbikes-notebook.ipynb")))
        )
      )

      val commit7Activity = Activity(
        CommitId("000007"),
        CommittedDate(commit6Activity.committedDate.value.plus(1, DAYS)),
        committer = persons.generateOne,
        project,
        agent,
        comment = "added refactored scripts",
        maybeInformedBy = Some(commit6Activity),
        maybeGenerationFactories = List(
          Generation.factory(entityFactory = Entity.factory(plotData)),
          Generation.factory(entityFactory = Entity.factory(cleanData))
        )
      )

      val oldCommit8ProcessRun = Activity(
        commitIds.generateOne,
        CommittedDate(commit7Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "1st-renku-run.cwl generation",
        maybeInformedBy = Some(commit7Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("1st-renku-run.cwl")))
        )
      )

      val commit8ParquetEntityFactory = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val commit8ProcessRun = ProcessRun.standAlone(
        CommitId("000008"),
        CommittedDate(commit7Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = s"renku run: committing 1 newly added files",
        maybeInformedBy = Some(commit7Activity),
        associationFactory = Association.process(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml("1st-renku-run.yaml"),
            Command("python"),
            inputs = List(
              Input.from(commit7Activity.entity(cleanData)),
              Input.from(commit3AddingDataSetFile.entity(dataSetFolder))
            ),
            outputs = List(Output.factory(commit8ParquetEntityFactory))
          )
        ),
        maybeInvalidation = oldCommit8ProcessRun.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val oldCommit9ProcessRun = Activity(
        commitIds.generateOne,
        CommittedDate(commit8ProcessRun.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "2nd-renku-run.cwl generation",
        maybeInformedBy = Some(commit7Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("2nd-renku-run.cwl")))
        )
      )

      val commit9GridPlotEntityFactory = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      val commit9ProcessRun = ProcessRun.standAlone(
        CommitId("000009"),
        CommittedDate(commit8ProcessRun.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = s"renku run: committing 1 newly added files",
        maybeInformedBy = Some(commit8ProcessRun),
        associationFactory = Association.process(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml("2nd-renku-run.yaml"),
            Command("python"),
            inputs = List(Input.from(commit7Activity.entity(plotData)),
                          Input.from(commit8ProcessRun.processRunAssociation.runPlan.output(bikesParquet))
            ),
            outputs = List(
              Output.factory(activity => Entity(Generation(cumulativePng, activity))),
              Output.factory(commit9GridPlotEntityFactory)
            )
          )
        ),
        maybeInvalidation = oldCommit9ProcessRun.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val commit10Activity = Activity(
        CommitId("0000010"),
        CommittedDate(commit9ProcessRun.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit9ProcessRun),
        maybeGenerationFactories = List(
          Generation.factory(Collection.factory(dataSetFolder, List(velo2019Location, velo2018Location)))
        )
      )

      val commit11Activity = Activity(
        CommitId("0000011"),
        CommittedDate(commit10Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku dataset add zhbikes velo.csv",
        maybeInformedBy = Some(commit10Activity),
        maybeGenerationFactories = List(
          dataSetGenerationFactory(
            partsFactories = List(
              DataSetPart.factory(
                datasets.PartName("2019velo.csv"),
                datasets.PartLocation(velo2019Location.value),
                datasetUrls.generateOption
              )
            )
          )
        )
      )

      val oldCommit12Workflow = Activity(
        commitIds.generateOne,
        CommittedDate(commit11Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku-update.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-update.cwl")))
        )
      )

      val oldCommit12WorkflowStep0 = Activity(
        commitIds.generateOne,
        CommittedDate(commit11Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku-migrate-step0.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-migrate-step0.cwl")))
        )
      )

      val oldCommit12WorkflowStep1 = Activity(
        commitIds.generateOne,
        CommittedDate(commit11Activity.committedDate.value.plus(1, DAYS)),
        persons.generateOne,
        project,
        agent,
        comment = "renku-migrate-step1.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-migrate-step1.cwl")))
        )
      )

      val commit12CumulativePngEntityFactory = (activity: Activity) => Entity(Generation(cumulativePng, activity))
      val commit12GridPlotPngEntityFactory   = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      val commit12ParquetEntityFactory       = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val commit12Committer                  = persons.generateOne
      val commit12Workflow = WorkflowRun(
        CommitId("0000012"),
        CommittedDate(commit11Activity.committedDate.value.plus(1, DAYS)),
        commit12Committer,
        project,
        agent,
        comment = "renku update: committing 2 newly added files",
        WorkflowFile.yaml("renku-update.yaml"),
        informedBy = commit11Activity,
        associationFactory = Association.workflow(
          agent.copy(cliVersion = cliVersions.generateOne),
          RunPlan.workflow(
            inputs = List(
              Input.from(commit7Activity.entity(cleanData), usedIn = Step.one),
              Input.from(commit10Activity.entity(dataSetFolder), usedIn = Step.one),
              Input.from(commit7Activity.entity(plotData), usedIn = Step.two)
            ),
            outputs = List(
              Output.factory(commit12ParquetEntityFactory, producedBy = Step.one),
              Output.factory(commit12CumulativePngEntityFactory, producedBy = Step.two),
              Output.factory(commit12GridPlotPngEntityFactory, producedBy = Step.two)
            ),
            subprocesses = List(
              commit8ProcessRun.processRunAssociation.runPlan,
              commit9ProcessRun.processRunAssociation.runPlan
            )
          )
        ),
        processRunsFactories = List(
          ProcessRun.child(
            associationFactory = Association.child(
              agent.copy(cliVersion = cliVersions.generateOne)
            ),
            maybeInvalidation = oldCommit12WorkflowStep0.generations.headOption.flatMap(_.maybeReverseEntity)
          ),
          ProcessRun.child(
            associationFactory = Association.child(
              agent.copy(cliVersion = cliVersions.generateOne)
            ),
            maybeInvalidation = oldCommit12WorkflowStep1.generations.headOption.flatMap(_.maybeReverseEntity)
          )
        ),
        maybeInvalidation = oldCommit12Workflow.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val examplarData = ExemplarData(
        gridPlotPng,
        commit12Workflow.commitId,
        commit12Committer,
        NodeDef(commit3AddingDataSetFile.entity(dataSetFolder)),
        NodeDef(commit7Activity.entity(plotData)),
        NodeDef(commit7Activity.entity(cleanData)),
        NodeDef(commit8ProcessRun),
        commit8ProcessRun.committedDate.value,
        NodeDef(commit8ProcessRun.processRunAssociation.runPlan.output(bikesParquet)),
        NodeDef(commit9ProcessRun),
        commit9ProcessRun.committedDate.value,
        NodeDef(commit9ProcessRun.processRunAssociation.runPlan.output(gridPlotPng)),
        NodeDef(commit9ProcessRun.processRunAssociation.runPlan.output(cumulativePng)),
        NodeDef(commit10Activity.entity(dataSetFolder)),
        NodeDef(commit12Workflow.processRunAssociation.runPlan.output(bikesParquet)),
        commit12Workflow.committedDate.value
      )

      List(
        commit3AddingDataSetFile.asJsonLD,
        commit5Activity.asJsonLD,
        commit6Activity.asJsonLD,
        commit7Activity.asJsonLD,
        oldCommit8ProcessRun.asJsonLD,
        commit8ProcessRun.asJsonLD,
        oldCommit9ProcessRun.asJsonLD,
        commit9ProcessRun.asJsonLD,
        commit10Activity.asJsonLD,
        commit11Activity.asJsonLD,
        commit12Workflow.asJsonLD,
        oldCommit12Workflow.asJsonLD,
        oldCommit12WorkflowStep0.asJsonLD,
        oldCommit12WorkflowStep1.asJsonLD
      ) -> examplarData
    }
  }

  private val projectCreators: Gen[Person] = for {
    name  <- userNames
    email <- userEmails
  } yield Person(name, email)

  final case class NodeDef(location: String, label: String, types: Set[String])

  object NodeDef {

    def apply(
        entity:              Entity with Artifact
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NodeDef =
      NodeDef(
        entity.location.value,
        entity.location.value,
        entity.asJsonLD.entityTypes
          .map(_.toList.map(_.toString))
          .getOrElse(throw new Exception("No entityTypes found"))
          .toSet
      )

    def apply(
        activity:            Activity with StandAloneProcessRun
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NodeDef =
      NodeDef(
        activity.processRunAssociation.runPlan.asJsonLD.entityId
          .getOrElse(throw new Exception("Non entity id found for ProcessRun"))
          .toString,
        activity.processRunAssociation.runPlan.asLabel,
        activity.asJsonLD.entityTypes
          .map(_.toList.map(_.toString))
          .getOrElse(throw new Exception("No entityTypes found"))
          .toSet
      )

    private implicit class RunPlanOps(runPlan: ProcessRunPlan) {

      lazy val asLabel: String =
        (runPlan.runArguments ++ runPlan.runCommandInputs ++ runPlan.runCommandOutputs)
          .foldLeft(List.empty[(Position, String)]) {
            case (allParams, parameter: Mapping) =>
              parameter.mappedTo match {
                case _: IOStream.StdIn.type  => (Position.first  -> asString(parameter)) +: allParams
                case _: IOStream.StdOut.type => (Position.second -> asString(parameter)) +: allParams
                case _: IOStream.StdErr.type => (Position.third  -> asString(parameter)) +: allParams
              }
            case (allParams, parameter: PositionInfo) => (parameter.position -> asString(parameter)) +: allParams
            case (allParams, _) => allParams
          }
          .sortBy(_._1)
          .map(_._2)
          .mkString(s"${runPlan.runCommand} ", " ", "")
          .trim

      private def asString(parameter: CommandParameter): String = parameter match {
        case param: Mapping =>
          param.maybePrefix.fold(s"${asSign(param)} ${param.value}")(prefix =>
            s"$prefix ${asSign(param)} ${param.value}"
          )
        case param => param.maybePrefix.fold(param.value.toString)(prefix => s"$prefix${param.value}")
      }

      private def asSign(parameter: Mapping): String = parameter.mappedTo match {
        case _: IOStream.StdIn.type  => "<"
        case _: IOStream.StdOut.type => ">"
        case _: IOStream.StdErr.type => "2>"
      }
    }
  }
}
