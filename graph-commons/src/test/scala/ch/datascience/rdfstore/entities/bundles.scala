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

package ch.datascience.rdfstore.entities

import Location._
import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{listOf, setOf}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PartLocation, PartName, PublishedDate, SameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.graph.model.{SchemaVersion, datasets, projects}
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.DataSetPart.dataSetParts
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.{Argument, Command, ProcessOrder}
import ch.datascience.rdfstore.{FusekiBaseUrl, Schemas}
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalacheck.Gen

object bundles extends Schemas {

  implicit lazy val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")

  def fileCommit(
      location:      Location      = locations.generateOne,
      commitId:      CommitId      = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person        = Person(userNames.generateOne, userEmails.generateOne),
      schemaVersion: SchemaVersion = schemaVersions.generateOne
  )(
      projectPath:         Path = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
      maybeParent:         Option[Project] = None
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    Entity(
      Generation(
        location,
        Activity(
          commitId,
          committedDate,
          committer,
          Project(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeParent),
          Agent(schemaVersion)
        )
      )
    ).asJsonLD

  def randomDataSetCommit(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    dataSetCommit()()()

  def dataSetCommit(
      commitId:      CommitId      = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person        = Person(userNames.generateOne, userEmails.generateOne),
      schemaVersion: SchemaVersion = schemaVersions.generateOne
  )(
      projectPath:         Path                 = projectPaths.generateOne,
      projectName:         projects.Name        = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      maybeProjectCreator: Option[Person]       = projectCreators.generateOption,
      maybeParent:         Option[Project]      = None
  )(
      datasetIdentifier:         Identifier = datasetIdentifiers.generateOne,
      datasetName:               Name = datasetNames.generateOne,
      maybeDatasetUrl:           Option[Url] = Gen.option(datasetUrls).generateOne,
      maybeDatasetSameAs:        Option[SameAs] = Gen.option(datasetSameAs).generateOne,
      maybeDatasetDescription:   Option[Description] = Gen.option(datasetDescriptions).generateOne,
      maybeDatasetPublishedDate: Option[PublishedDate] = Gen.option(datasetPublishedDates).generateOne,
      datasetCreatedDate:        datasets.DateCreated = datasets.DateCreated(committedDate.value),
      datasetCreators:           Set[Person] = setOf(persons).generateOne,
      datasetParts:              List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne
  )(implicit renkuBaseUrl:       RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD = {
    val project: Project = Project(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeParent)
    DataSet(
      datasetIdentifier,
      datasetName,
      maybeDatasetUrl,
      maybeDatasetSameAs,
      maybeDatasetDescription,
      maybeDatasetPublishedDate,
      datasetCreatedDate,
      datasetCreators,
      datasetParts map { case (name, location) => DataSetPart(name, location, commitId, project) },
      Generation(Location(".renku") / "datasets" / datasetIdentifier,
                 Activity(
                   commitId,
                   committedDate,
                   committer,
                   project,
                   Agent(schemaVersion)
                 )),
      project
    ).asJsonLD
  }

  object exemplarLineageFlow {

    final case class ExamplarData(
        commitId:                   CommitId,
        location:                   Location,
        `sha3 zhbikes`:             NodeDef,
        `sha7 plot_data`:           NodeDef,
        `sha7 clean_data`:          NodeDef,
        `sha8 renku run`:           NodeDef,
        `sha8 parquet`:             NodeDef,
        `sha9 renku run`:           NodeDef,
        `sha9 plot_data`:           NodeDef,
        `sha10 zhbikes`:            NodeDef,
        `sha12 step1 renku update`: NodeDef,
        `sha12 step2 renku update`: NodeDef,
        `sha12 step2 grid_plot`:    NodeDef,
        `sha12 parquet`:            NodeDef
    )

    object ExamplarData {
      def apply(commitId:                CommitId,
                location:                Location,
                `sha3 zhbikes`:          JsonLD,
                `sha7 plot_data`:        JsonLD,
                `sha7 clean_data`:       JsonLD,
                `sha8 renku run`:        JsonLD,
                `sha8 parquet`:          JsonLD,
                `sha9 renku run`:        JsonLD,
                `sha9 plot_data`:        JsonLD,
                `sha10 zhbikes`:         JsonLD,
                `sha12 step1 renku`:     JsonLD,
                `sha12 step2 renku`:     JsonLD,
                `sha12 step2 grid_plot`: JsonLD,
                `sha12 parquet`:         JsonLD): ExamplarData =
        ExamplarData(
          commitId,
          location,
          `sha3 zhbikes`             = NodeDef(`sha3 zhbikes`, label          = "data/zhbikes@000003"),
          `sha7 plot_data`           = NodeDef(`sha7 plot_data`, label        = "src/plot_data.py@000007"),
          `sha7 clean_data`          = NodeDef(`sha7 clean_data`, label       = "src/clean_data.py@000007"),
          `sha8 renku run`           = NodeDef(`sha8 renku run`, label        = "renku run python"),
          `sha8 parquet`             = NodeDef(`sha8 parquet`, label          = "data/preprocessed/zhbikes.parquet@000008"),
          `sha9 renku run`           = NodeDef(`sha9 renku run`, label        = "renku run python"),
          `sha9 plot_data`           = NodeDef(`sha9 plot_data`, label        = "figs/grid_plot.png@000009"),
          `sha10 zhbikes`            = NodeDef(`sha10 zhbikes`, label         = "data/zhbikes@0000010"),
          `sha12 step1 renku update` = NodeDef(`sha12 step1 renku`, label     = "renku update"),
          `sha12 step2 renku update` = NodeDef(`sha12 step2 renku`, label     = "renku update"),
          `sha12 step2 grid_plot`    = NodeDef(`sha12 step2 grid_plot`, label = "figs/grid_plot.png@0000012"),
          `sha12 parquet`            = NodeDef(`sha12 parquet`, label         = "data/preprocessed/zhbikes.parquet@0000012")
        )
    }

    final case class NodeDef(id: String, location: String, label: String, types: Set[String])

    object NodeDef {
      import io.renku.jsonld.JsonLDDecoder._

      def apply(entityJson: JsonLD, label: String): NodeDef = NodeDef(
        entityJson.entityId
          .getOrElse(throw new Exception("No entityId found"))
          .toString,
        entityJson.cursor
          .downField(prov / "atLocation")
          .as[String]
          .fold(error => throw new Exception(error.message), identity),
        label,
        entityJson.entityTypes
          .map(_.toList.map(_.toString))
          .getOrElse(throw new Exception("No entityTypes found"))
          .toSet
      )
    }

    def apply(
        projectPath:         Path = projectPaths.generateOne,
        schemaVersion:       SchemaVersion = schemaVersions.generateOne
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): (List[JsonLD], ExamplarData) = {
      val project =
        Project(projectPath, projectNames.generateOne, projectCreatedDates.generateOne, projectCreators.generateOption)
      val agent     = Agent(schemaVersion)
      val dataSetId = datasets.Identifier("d67a1653-0b6e-463b-89a0-afe72a53c8bb")

      val commit3Id        = CommitId("000003")
      val commit7Id        = CommitId("000007")
      val commit8Id        = CommitId("000008")
      val commit9Id        = CommitId("000009")
      val commit10Id       = CommitId("0000010")
      val commit12Id       = CommitId("0000012")
      val dataSetFolder    = Location("data/zhbikes")
      val plotData         = Location("src/plot_data.py")
      val cleanData        = Location("src/clean_data.py")
      val bikesParquet     = Location("data/preprocessed/zhbikes.parquet")
      val cumulativePng    = Location("figs/cumulative.png")
      val gridPlotPng      = Location("figs/grid_plot.png")
      val velo2018Location = Location("data/zhbikes/2018velo.csv")
      val velo2019Location = Location("data/zhbikes/2019velo.csv")

      val commit3DataSetFolderCollection = Collection(
        commit3Id,
        dataSetFolder,
        project,
        members = List(Entity(commit3Id, velo2019Location, project))
      )
      val commit5Activity = Activity(
        CommitId("000005"),
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment = "packages installed",
        maybeInformedBy = Some(
          Activity(CommitId("000004"),
                   committedDates.generateOne,
                   persons.generateOne,
                   project,
                   agent,
                   comment = "renku dataset add zhbikes")
        )
      )
      val commit6Activity = Activity(
        CommitId("000006"),
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "added notebook",
        maybeInformedBy = Some(commit5Activity)
      )
      val commit7Activity = Activity(
        commit7Id,
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "added refactored scripts",
        maybeInformedBy = Some(commit6Activity)
      )
      val commit7PlotDataEntity  = Entity(commit7Id, plotData, project)
      val commit7CleanDataEntity = Entity(commit7Id, cleanData, project)

      val commit8CommandCleanDataInput     = Input.from(commit7CleanDataEntity)
      val commit8CommandDataSetFolderInput = Input.from(commit3DataSetFolderCollection)
      val commit8ParquetEntityFactory      = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val commit8ParquetEntity             = commit8ParquetEntityFactory(commit8ProcessRun)
      lazy val commit8ProcessRun = ProcessRun(
        commit8Id,
        committedDates.generateOne,
        persons.generateOne,
        agent,
        comment         = s"renku run python $plotData $dataSetFolder $bikesParquet",
        maybeInformedBy = Some(commit7Activity),
        association = Association(
          commit8Id,
          agent.copy(schemaVersion = schemaVersions.generateOne),
          RunPlan(
            WorkflowFile.yaml("1st-renku-run.yaml"),
            project,
            Command("python"),
            inputs = List(commit8CommandCleanDataInput, commit8CommandDataSetFolderInput),
            outputs = List(
              Output.from(activity = commit8ProcessRun, commit8ParquetEntityFactory)
            )
          )
        ),
        usages = List(
          Usage(commit8CommandCleanDataInput(Position(1))),
          Usage(commit8CommandDataSetFolderInput(Position(2)))
        )
      )
      val commit8Cwl = Entity(commitIds.generateOne,
                              WorkflowFile.cwl("1st-renku-run.cwl"),
                              project,
                              maybeInvalidationActivity = Some(commit8ProcessRun))

      val commit9CommandPlotDataInput  = Input.from(commit7PlotDataEntity)
      val commit9CommandParquetInput   = Input.from(commit8ParquetEntity)
      val commit9GridPlotEntityFactory = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      lazy val commit9ProcessRun = ProcessRun(
        commit9Id,
        committedDates.generateOne,
        persons.generateOne,
        agent,
        comment         = s"renku run python $plotData $bikesParquet",
        maybeInformedBy = Some(commit8ProcessRun),
        association = Association.factory(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            WorkflowFile.yaml("2nd-renku-run.yaml"),
            project,
            Command("python"),
            inputs = List(commit9CommandPlotDataInput, commit9CommandParquetInput),
            outputs = List(
              Output.from(activity = commit9ProcessRun, activity => Entity(Generation(cumulativePng, activity))),
              Output.from(activity = commit9ProcessRun, commit9GridPlotEntityFactory)
            )
          )
        ),
        usages = List(
          Usage(commit9CommandPlotDataInput(Position(1))),
          Usage(commit9CommandParquetInput(Position(2)))
        )
      )
      val commit9GridPlotEntity = commit9GridPlotEntityFactory(commit9ProcessRun)
      val commit9Cwl = Entity(commitIds.generateOne,
                              WorkflowFile.cwl("2nd-renku-run.cwl"),
                              project,
                              maybeInvalidationActivity = Some(commit9ProcessRun))

      val commit10Activity = Activity(
        commit10Id,
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit9ProcessRun)
      )
      val commit10DataSetFolderCollection = Collection(
        commit10Id,
        dataSetFolder,
        project,
        members = List(Entity(commit3Id, velo2019Location, project), Entity(commit10Id, velo2018Location, project))
      )
      val commit11Activity = Activity(
        CommitId("0000011"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku dataset add zhbikes velo.csv",
        maybeInformedBy = Some(commit10Activity)
      )

      val commit12CommandPlotDataInput       = Input.from(commit7PlotDataEntity)
      val commit12CommandCleanDataInput      = Input.from(commit7CleanDataEntity)
      val commit12CommandDataSetFolderInput  = Input.from(commit10DataSetFolderCollection)
      val commit12CumulativePngEntityFactory = (activity: Activity) => Entity(Generation(cumulativePng, activity))
      val commit12GridPlotPngEntityFactory   = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      val commit12ParquetEntityFactory       = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val runPlanStep0 = (activity: Activity) =>
        RunPlan.process(
          WorkflowFile.yaml("renku-migrate-step0.yaml"),
          project,
          Command("python"),
          inputs = List(commit12CommandCleanDataInput, commit12CommandDataSetFolderInput),
          outputs = List(
            Output.from(commit12ParquetEntityFactory)
          ),
          maybeProcessOrder = Some(ProcessOrder(0))
        )

      lazy val commit12Workflow = WorkflowRun(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
        comment = "renku update",
        WorkflowFile.yaml("renku-update.yaml"),
        informedBy = commit11Activity,
        Association.factory(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan.workflow(
            project,
            inputs = List(
              commit12CommandPlotDataInput,
              commit12CommandCleanDataInput,
              commit12CommandDataSetFolderInput
            ),
            outputs = List(
              Output.from(activity = commit12Workflow, commit12CumulativePngEntityFactory),
              Output.from(activity = commit12Workflow, commit12GridPlotPngEntityFactory),
              Output.from(activity = commit12Workflow, commit12ParquetEntityFactory)
            ),
            subprocesses = List(runPlanStep0)
          )
        ),
        usages = List(
          Usage(commit12CommandPlotDataInput(Position(1))),
          Usage(commit12CommandCleanDataInput(Position(2))),
          Usage(commit12CommandDataSetFolderInput(Position(3)))
        )
      )
      val commit12WorkflowCwl = Entity(commitIds.generateOne,
                                       WorkflowFile.cwl("renku-update.cwl"),
                                       project,
                                       maybeInvalidationActivity = Some(commit12Workflow))

      val commit12Step0CleanDataInput     = Input.from(commit7CleanDataEntity)
      val commit12Step0DataSetFolderInput = Input.from(commit10DataSetFolderCollection)
      val commit12Step0ProcessRun = ProcessRun(
        workflowRun     = commit12Workflow,
        commitId        = commit12Id,
        step            = Step(0),
        committedDate   = committedDates.generateOne,
        committer       = persons.generateOne,
        agent           = agent,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        associationFactory = Association.factory(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          runPlanStep0
        ),
        usagesFactories = List(
          Usage.factory(commit12Step0CleanDataInput(Position(1))),
          Usage.factory(commit12Step0DataSetFolderInput(Position(2)))
        )
      )
      val commit12Step0Cwl = Entity(commitIds.generateOne,
                                    WorkflowFile.cwl("renku-migrate-step0.cwl"),
                                    project,
                                    maybeInvalidationActivity = Some(commit12Step0ProcessRun))

      val commit12Step1PlotDataInput = Input.from(commit7PlotDataEntity)
      val commit12Step1ParquetInput  = Input.from(commit12ParquetEntityFactory(commit12Step0ProcessRun))
      val commit12Step1ProcessRun = ProcessRun(
        workflowRun     = commit12Workflow,
        commitId        = commit12Id,
        step            = Step(1),
        committedDate   = committedDates.generateOne,
        committer       = persons.generateOne,
        agent           = agent,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        associationFactory = Association.factory(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            WorkflowFile.yaml("renku-migrate-step1.yaml"),
            project,
            Command("python"),
            inputs = List(commit12Step1PlotDataInput, commit12Step1ParquetInput),
            outputs = List(
              Output.from(activity = commit12Step1ProcessRun, commit12CumulativePngEntityFactory),
              Output.from(activity = commit12Step1ProcessRun, commit12GridPlotPngEntityFactory)
            ),
            maybeProcessOrder = Some(ProcessOrder(1))
          )
        ),
        usagesFactories = List(
          Usage.factory(commit12Step1PlotDataInput(Position(1))),
          Usage.factory(commit12Step1ParquetInput(Position(2)))
        )
      )
      val commit12GridPlotPngEntity = commit12GridPlotPngEntityFactory(commit12Step1ProcessRun)
      val commit12Step1Cwl = Entity(commitIds.generateOne,
                                    WorkflowFile.cwl("renku-migrate-step1.cwl"),
                                    project,
                                    maybeInvalidationActivity = Some(commit12Step1ProcessRun))

      val examplarData = ExamplarData(
        commit12Id,
        gridPlotPng,
        commit3DataSetFolderCollection.asJsonLD,
        commit7PlotDataEntity.asJsonLD,
        commit7CleanDataEntity.asJsonLD,
        commit8ProcessRun.asJsonLD,
        commit8ParquetEntity.asJsonLD,
        commit9ProcessRun.asJsonLD,
        commit9GridPlotEntity.asJsonLD,
        commit10DataSetFolderCollection.asJsonLD,
        commit12Step0ProcessRun.asJsonLD,
        commit12Step0Cwl.asJsonLD,
        commit12ParquetEntityFactory.asJsonLD,
        commit12Step1ProcessRun.asJsonLD,
        commit12WorkflowCwl.asJsonLD,
        commit12CumulativePngEntityFactory.asJsonLD,
        commit12GridPlotPngEntity.asJsonLD
      )

      List(
        Entity(
          Generation(
            velo2019Location,
            Activity(
              commit3Id,
              committedDates.generateOne,
              committer = persons.generateOne,
              project,
              agent,
              comment = "renku dataset: committing 1 newly added files",
              maybeInformedBy = Some(
                Activity(CommitId("000002"),
                         committedDates.generateOne,
                         persons.generateOne,
                         project,
                         agent,
                         comment = "renku dataset create zhbikes")
              )
            )
          )
        ).asJsonLD,
        Entity(Generation(Location("requirements.txt"), commit5Activity)).asJsonLD,
        Entity(Generation(Location("notebooks/zhbikes-notebook.ipynb"), commit6Activity)).asJsonLD,
        Entity(Generation(plotData, commit7Activity)).asJsonLD,
        Entity(bikesParquet, Generation(Location("outputs/output_0"), commit8ProcessRun)).asJsonLD,
        commit8Cwl.asJsonLD,
        commit9Cwl.asJsonLD,
        Entity(outFile1, Generation(Location("outputs/output_0"), commit9ProcessRun)).asJsonLD,
        commit9GridPlotEntity.asJsonLD,
        Entity(Generation(velo2018Location, commit10Activity)).asJsonLD,
        DataSet(
          id          = dataSetId,
          name        = datasets.Name("zhbikes"),
          createdDate = datasetCreatedDates.generateOne,
          creators    = Set(persons.generateOne),
          parts = List(
            DataSetPart(
              datasets.PartName("2019velo.csv"),
              datasets.PartLocation(velo2019Location.value),
              commit3Id,
              project,
              datasetCreatedDates.generateOne,
              datasetUrls.generateOption
            ),
            DataSetPart(
              datasets.PartName("2018velo.csv"),
              datasets.PartLocation(velo2018Location.value),
              commit10Id,
              project,
              datasetCreatedDates.generateOne,
              datasetUrls.generateOption
            )
          ),
          generation = Generation(Location(s".renku/datasets/$dataSetId"), commit11Activity),
          project    = project
        ).asJsonLD,
        commit12ParquetEntityFactory.asJsonLD,
        commit12Step0Cwl.asJsonLD,
        Entity(outFile1, Generation(Location("steps/step_1/outputs/output_0"), commit12Step0ProcessRun)).asJsonLD,
        commit12ParquetEntityFactory.asJsonLD,
        commit12Step1GridPlotEntity.asJsonLD,
        Entity(bikesParquet, Generation(Location("steps/step_2/outputs/output_0"), commit12Workflow)).asJsonLD,
        Entity(outFile1, Generation(Location("steps/step_1/outputs/output_0"), commit12Workflow)).asJsonLD,
        Entity(outFile2, Generation(Location("steps/step_1/outputs/output_1"), commit12Workflow)).asJsonLD
      ) -> examplarData
    }
  }

  private val projectCreators: Gen[Person] = for {
    name  <- userNames
    email <- userEmails
  } yield Person(name, email)
}
