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

import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{listOf, setOf}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PartLocation, PartName, PublishedDate, SameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, FilePath, Path}
import ch.datascience.graph.model.{SchemaVersion, datasets, projects}
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.DataSetPart.dataSetParts
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.{Argument, Command}
import ch.datascience.rdfstore.{FusekiBaseUrl, Schemas}
import eu.timepit.refined.auto._
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax._
import org.scalacheck.Gen

object bundles extends Schemas {

  implicit lazy val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")

  def fileCommit(
      filePath:      FilePath      = filePaths.generateOne,
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
        filePath,
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
      datasetParts map { case (name, location) => DataSetPart(name, location, commitId, project, committer) },
      Generation(FilePath(".renku") / "datasets" / datasetIdentifier,
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
        filePath:                   FilePath,
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
                filePath:                FilePath,
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
          filePath,
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

      val commit3Id  = CommitId("000003")
      val commit7Id  = CommitId("000007")
      val commit8Id  = CommitId("000008")
      val commit9Id  = CommitId("000009")
      val commit10Id = CommitId("0000010")
      val commit12Id = CommitId("0000012")
      val outFile1   = FilePath("figs/cumulative.png")
      val outFile2   = FilePath("figs/grid_plot.png")

      val commit3EntityCollection = EntityCollection(
        commit3Id,
        FilePath("data/zhbikes"),
        project,
        members = List(Entity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project))
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
      val commit7PlotDataEntity  = Entity(commit7Id, FilePath("src/plot_data.py"), project)
      val commit7CleanDataEntity = Entity(commit7Id, FilePath("src/clean_data.py"), project)
      val commit8CommandInput1   = Input(Position(1), Value("inputs/input_1"), None, Nil)
      val commit8CommandInput2   = Input(Position(2), Value("inputs/input_2"), None, Nil)
      val commit8ProcessRunActivity = ProcessRun(
        commit8Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku run python",
        maybeInformedBy = Some(commit7Activity),
        association = Association(
          commit8Id,
          agent.copy(maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            commit8Id,
            WorkflowFile.cwl("3983f12670a8410eb9b15d81e8949560_python.cwl"),
            project,
            Command("run"),
            List(Argument("python")),
            List(commit8CommandInput1, commit8CommandInput2),
            Nil
          )
        ),
        usages = List(
          Usage(commit8Id, commit8CommandInput1, commit7CleanDataEntity),
          Usage(commit8Id, commit8CommandInput2, commit3EntityCollection)
        )
      )
      val commit8ParquetEntity = Entity(commit8Id, FilePath("data/preprocessed/zhbikes.parquet"), project)

      val commit9CommandInput1 = Input(Position(1), Value("inputs/input_1"), None, Nil)
      val commit9CommandInput2 = Input(Position(2), Value("inputs/input_2"), None, Nil)
      val commit9ProcessRunActivity = ProcessRun(
        commit9Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku run python",
        maybeInformedBy = Some(commit8ProcessRunActivity),
        association = Association(
          commit9Id,
          agent.copy(maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            commit8Id,
            WorkflowFile.cwl("fdf4672a1f424758af8fdb590e0d7869_python.cwl"),
            project,
            Command("run"),
            List(Argument("python")),
            List(commit9CommandInput1, commit9CommandInput1),
            Nil
          )
        ),
        usages = List(
          Usage(commit9Id, commit9CommandInput1, commit7PlotDataEntity),
          Usage(commit9Id, commit9CommandInput2, commit8ParquetEntity)
        )
      )
      val commit9PlotDataEntity = Entity(
        outFile2,
        Generation(FilePath("outputs/output_1"), commit9ProcessRunActivity)
      )
      val commit10Activity = Activity(
        commit10Id,
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit9ProcessRunActivity)
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
      val commit10ZhbikesCollectionEntity = EntityCollection(
        commit10Id,
        FilePath("data/zhbikes"),
        project,
        members = List(Entity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project),
                       Entity(commit10Id, FilePath("data/zhbikes/2018velo.csv"), project))
      )

      val commit12CommandInput1 = Input(Position(1), Value("inputs/input_1"), None, Nil)
      val commit12CommandInput2 = Input(Position(2), Value("inputs/input_2"), None, Nil)
      val commit12CommandInput3 = Input(Position(3), Value("inputs/input_3"), None, Nil)
      val commit12RunWorkflowActivity = WorkflowRun(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
        comment = "renku update",
        WorkflowFile.cwl("db4f249682b34e0db239cc26945a126c.cwl"),
        informedBy = commit11Activity,
        Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            commit12Id,
            WorkflowFile.cwl("db4f249682b34e0db239cc26945a126c.cwl"),
            project,
            Command("update"),
            Nil,
            List(commit12CommandInput1, commit12CommandInput2, commit12CommandInput3),
            Nil
          )
        ),
        usages = List(
          Usage(commit12Id, commit12CommandInput1, commit7PlotDataEntity),
          Usage(commit12Id, commit12CommandInput2, commit7CleanDataEntity),
          Usage(
            commit12Id,
            commit12CommandInput3,
            commit10ZhbikesCollectionEntity
          )
        )
      )

      val commit12Step1CommandInput1 = Input(Position(1), Value("steps/step_1/inputs/input_1"), None, Nil)
      val commit12Step1CommandInput2 = Input(Position(2), Value("steps/step_1/inputs/input_2"), None, Nil)
      val commit12Step1ProcessRunActivity = ProcessRun(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        association = Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            commit9Id,
            WorkflowFile.cwl("fdf4672a1f424758af8fdb590e0d7869_python.cwl"),
            project,
            Command("update"),
            Nil,
            List(commit12Step1CommandInput1, commit12Step1CommandInput2),
            Nil
          ),
          maybeStep = Some("steps/step_2")
        ),
        usages = List(
          Usage(commit12Id, commit12Step1CommandInput1, commit7PlotDataEntity),
          Usage(commit12Id,
                commit12Step1CommandInput2,
                Entity(commit12Id, FilePath("data/preprocessed/zhbikes.parquet"), project))
        ),
        maybeStepId               = Some("steps/step_1"),
        maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
      )

      val commit12Step2CommandInput1 = Input(Position(1), Value("steps/step_2/inputs/input_1"), None, Nil)
      val commit12Step2CommandInput2 = Input(Position(2), Value("steps/step_2/inputs/input_2"), None, Nil)
      val commit12Step2ProcessRunActivity = ProcessRun(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        association = Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan(
            commit8Id,
            WorkflowFile.cwl("3983f12670a8410eb9b15d81e8949560_python.cwl"),
            project,
            Command("update"),
            Nil,
            List(commit12Step2CommandInput1, commit12Step2CommandInput2),
            Nil
          ),
          maybeStep = Some("steps/step_2")
        ),
        usages = List(
          Usage(commit12Id, commit12Step2CommandInput1, commit7CleanDataEntity),
          Usage(
            commit12Id,
            commit12Step2CommandInput2,
            commit10ZhbikesCollectionEntity
          )
        ),
        maybeStepId               = Some("steps/step_2"),
        maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
      )
      val commit12PlotDataEntity = Entity(
        outFile2,
        Generation(FilePath("steps/step_1/outputs/output_1"), commit12Step1ProcessRunActivity)
      )
      val commit12ParquetEntity = Entity(
        FilePath("data/preprocessed/zhbikes.parquet"),
        Generation(
          FilePath("steps/step_2/outputs/output_0"),
          commit12Step2ProcessRunActivity
        )
      )

      val examplarData = ExamplarData(
        commit12Id,
        outFile2,
        commit3EntityCollection.asJsonLD,
        commit7PlotDataEntity.asJsonLD,
        commit7CleanDataEntity.asJsonLD,
        commit8ProcessRunActivity.asJsonLD,
        commit8ParquetEntity.asJsonLD,
        commit9ProcessRunActivity.asJsonLD,
        commit9PlotDataEntity.asJsonLD,
        commit10ZhbikesCollectionEntity.asJsonLD,
        commit12Step1ProcessRunActivity.asJsonLD,
        commit12Step2ProcessRunActivity.asJsonLD,
        commit12PlotDataEntity.asJsonLD,
        commit12ParquetEntity.asJsonLD
      )

      List(
        Entity(
          Generation(
            FilePath("data/zhbikes/2019velo.csv"),
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
        Entity(Generation(FilePath("requirements.txt"), commit5Activity)).asJsonLD,
        Entity(Generation(FilePath("notebooks/zhbikes-notebook.ipynb"), commit6Activity)).asJsonLD,
        Entity(Generation(FilePath("src/plot_data.py"), commit7Activity)).asJsonLD,
        Entity(FilePath("data/preprocessed/zhbikes.parquet"),
               Generation(FilePath("outputs/output_0"), commit8ProcessRunActivity)).asJsonLD,
        Entity(outFile1, Generation(FilePath("outputs/output_0"), commit9ProcessRunActivity)).asJsonLD,
        commit9PlotDataEntity.asJsonLD,
        Entity(Generation(FilePath("data/zhbikes/2018velo.csv"), commit10Activity)).asJsonLD,
        DataSet(
          id          = dataSetId,
          name        = datasets.Name("zhbikes"),
          createdDate = datasetCreatedDates.generateOne,
          creators    = Set(persons.generateOne),
          parts = List(
            DataSetPart(
              datasets.PartName("2019velo.csv"),
              datasets.PartLocation("data/zhbikes/2019velo.csv"),
              commit3Id,
              project,
              persons.generateOne,
              datasetCreatedDates.generateOne,
              datasetUrls.generateOption
            ),
            DataSetPart(
              datasets.PartName("2018velo.csv"),
              datasets.PartLocation("data/zhbikes/2018velo.csv"),
              commit10Id,
              project,
              persons.generateOne,
              datasetCreatedDates.generateOne,
              datasetUrls.generateOption
            )
          ),
          generation = Generation(FilePath(s".renku/datasets/$dataSetId"), commit11Activity),
          project    = project
        ).asJsonLD,
        commit12ParquetEntity.asJsonLD,
        Entity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12Step1ProcessRunActivity)).asJsonLD,
        commit12PlotDataEntity.asJsonLD,
        Entity(FilePath("data/preprocessed/zhbikes.parquet"),
               Generation(FilePath("steps/step_2/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        Entity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        Entity(outFile2, Generation(FilePath("steps/step_1/outputs/output_1"), commit12RunWorkflowActivity)).asJsonLD
      ) -> examplarData
    }
  }

  private val projectCreators: Gen[Person] = for {
    name  <- userNames
    email <- userEmails
  } yield Person(name, email)
}
