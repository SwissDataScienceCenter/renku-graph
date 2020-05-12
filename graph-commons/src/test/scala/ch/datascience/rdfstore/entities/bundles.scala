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
import ch.datascience.graph.model.datasets.{DerivedFrom, Description, Identifier, Name, PartLocation, PartName, PublishedDate, SameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, FilePath, Path}
import ch.datascience.graph.model.{SchemaVersion, datasets, projects}
import ch.datascience.rdfstore.entities.DataSetPart.dataSetParts
import ch.datascience.rdfstore.entities.Person.persons
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
    ArtifactEntity(
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
    Gen.oneOf(nonModifiedDataSetCommit()()(), modifiedDataSetCommit()()()).generateOne

  def nonModifiedDataSetCommit(
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
      datasetUrl:                Url = datasetUrls.generateOne,
      maybeDatasetSameAs:        Option[SameAs] = Gen.option(datasetSameAs).generateOne,
      maybeDatasetDescription:   Option[Description] = Gen.option(datasetDescriptions).generateOne,
      maybeDatasetPublishedDate: Option[PublishedDate] = Gen.option(datasetPublishedDates).generateOne,
      datasetCreatedDate:        datasets.DateCreated = datasets.DateCreated(committedDate.value),
      datasetCreators:           Set[Person] = setOf(persons).generateOne,
      datasetParts:              List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne
  )(implicit renkuBaseUrl:       RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD = {
    val project: Project = Project(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeParent)
    DataSet
      .nonModified(
        datasetIdentifier,
        datasetName,
        datasetUrl,
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
      )
      .asJsonLD
  }

  def modifiedDataSetCommit(
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
      datasetUrl:                Url = datasetUrls.generateOne,
      datasetDerivedFrom:        DerivedFrom = datasetDerivedFroms.generateOne,
      maybeDatasetDescription:   Option[Description] = Gen.option(datasetDescriptions).generateOne,
      maybeDatasetPublishedDate: Option[PublishedDate] = Gen.option(datasetPublishedDates).generateOne,
      datasetCreatedDate:        datasets.DateCreated = datasets.DateCreated(committedDate.value),
      datasetCreators:           Set[Person] = setOf(persons).generateOne,
      datasetParts:              List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne
  )(implicit renkuBaseUrl:       RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD = {
    val project: Project = Project(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeParent)
    DataSet
      .modified(
        datasetIdentifier,
        datasetName,
        datasetUrl,
        datasetDerivedFrom,
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
      )
      .asJsonLD
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

      val commit3EntityCollection = ArtifactEntityCollection(
        commit3Id,
        FilePath("data/zhbikes"),
        project,
        members = List(ArtifactEntity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project))
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
      val commit7PlotDataEntity  = ArtifactEntity(commit7Id, FilePath("src/plot_data.py"), project)
      val commit7CleanDataEntity = ArtifactEntity(commit7Id, FilePath("src/clean_data.py"), project)
      val commit8ProcessRunActivity = ProcessRunActivity(
        commit8Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku run python",
        maybeInformedBy = Some(commit7Activity),
        association = Association(
          commit8Id,
          agent.copy(maybeStartedBy = Some(persons.generateOne)),
          StandardProcessPlan(commit8Id, CwlFile("3983f12670a8410eb9b15d81e8949560_python.cwl"), project)
        ),
        usages = List(
          Usage(commit8Id, FilePath("inputs/input_1"), commit7CleanDataEntity),
          Usage(commit8Id, FilePath("inputs/input_2"), commit3EntityCollection)
        )
      )
      val commit8ParquetEntity = ArtifactEntity(commit8Id, FilePath("data/preprocessed/zhbikes.parquet"), project)
      val commit9ProcessRunActivity = ProcessRunActivity(
        commit9Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku run python",
        maybeInformedBy = Some(commit8ProcessRunActivity),
        association = Association(
          commit9Id,
          agent.copy(maybeStartedBy = Some(persons.generateOne)),
          StandardProcessPlan(commit8Id, CwlFile("fdf4672a1f424758af8fdb590e0d7869_python.cwl"), project)
        ),
        usages = List(
          Usage(commit9Id, FilePath("inputs/input_1"), commit7PlotDataEntity),
          Usage(commit9Id, FilePath("inputs/input_2"), commit8ParquetEntity)
        )
      )
      val commit9PlotDataEntity = ArtifactEntity(
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
      val commit10ZhbikesCollectionEntity = ArtifactEntityCollection(
        commit10Id,
        FilePath("data/zhbikes"),
        project,
        members = List(ArtifactEntity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project),
                       ArtifactEntity(commit10Id, FilePath("data/zhbikes/2018velo.csv"), project))
      )
      val commit12RunWorkflowActivity = ProcessRunWorkflowActivity(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
        comment = "renku update",
        CwlFile("db4f249682b34e0db239cc26945a126c.cwl"),
        informedBy = commit11Activity,
        Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          ProcessPlanWorkflow(commit12Id, CwlFile("db4f249682b34e0db239cc26945a126c.cwl"), project)
        ),
        usages = List(
          Usage(commit12Id, FilePath("inputs/input_1"), commit7PlotDataEntity),
          Usage(commit12Id, FilePath("inputs/input_2"), commit7CleanDataEntity),
          Usage(
            commit12Id,
            FilePath("inputs/input_3"),
            commit10ZhbikesCollectionEntity
          )
        )
      )

      val commit12Step1ProcessRunActivity = ProcessRunActivity(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        association = Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          StandardProcessPlan(commit9Id, CwlFile("fdf4672a1f424758af8fdb590e0d7869_python.cwl"), project),
          maybeStep = Some("steps/step_2")
        ),
        usages = List(
          Usage(commit12Id, FilePath("steps/step_1/inputs/input_1"), commit7PlotDataEntity),
          Usage(commit12Id,
                FilePath("steps/step_1/inputs/input_2"),
                ArtifactEntity(commit12Id, FilePath("data/preprocessed/zhbikes.parquet"), project))
        ),
        maybeStepId               = Some("steps/step_1"),
        maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
      )
      val commit12Step2ProcessRunActivity = ProcessRunActivity(
        commit12Id,
        committedDates.generateOne,
        persons.generateOne,
        comment         = "renku update",
        maybeInformedBy = Some(commit11Activity),
        association = Association(
          commit12Id,
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          StandardProcessPlan(commit8Id, CwlFile("3983f12670a8410eb9b15d81e8949560_python.cwl"), project),
          maybeStep = Some("steps/step_2")
        ),
        usages = List(
          Usage(commit12Id, FilePath("steps/step_2/inputs/input_1"), commit7CleanDataEntity),
          Usage(
            commit12Id,
            FilePath("steps/step_2/inputs/input_2"),
            commit10ZhbikesCollectionEntity
          )
        ),
        maybeStepId               = Some("steps/step_2"),
        maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
      )
      val commit12PlotDataEntity = ArtifactEntity(
        outFile2,
        Generation(FilePath("steps/step_1/outputs/output_1"), commit12Step1ProcessRunActivity)
      )
      val commit12ParquetEntity = ArtifactEntity(
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
        ArtifactEntity(
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
        ArtifactEntity(Generation(FilePath("requirements.txt"), commit5Activity)).asJsonLD,
        ArtifactEntity(Generation(FilePath("notebooks/zhbikes-notebook.ipynb"), commit6Activity)).asJsonLD,
        ArtifactEntity(Generation(FilePath("src/plot_data.py"), commit7Activity)).asJsonLD,
        ArtifactEntity(FilePath("data/preprocessed/zhbikes.parquet"),
                       Generation(FilePath("outputs/output_0"), commit8ProcessRunActivity)).asJsonLD,
        ArtifactEntity(outFile1, Generation(FilePath("outputs/output_0"), commit9ProcessRunActivity)).asJsonLD,
        commit9PlotDataEntity.asJsonLD,
        ArtifactEntity(Generation(FilePath("data/zhbikes/2018velo.csv"), commit10Activity)).asJsonLD,
        DataSet
          .nonModified(
            id          = dataSetId,
            name        = datasets.Name("zhbikes"),
            url         = datasetUrls.generateOne,
            maybeSameAs = None,
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
          )
          .asJsonLD,
        commit12ParquetEntity.asJsonLD,
        ArtifactEntity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12Step1ProcessRunActivity)).asJsonLD,
        commit12PlotDataEntity.asJsonLD,
        ArtifactEntity(FilePath("data/preprocessed/zhbikes.parquet"),
                       Generation(FilePath("steps/step_2/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        ArtifactEntity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        ArtifactEntity(outFile2, Generation(FilePath("steps/step_1/outputs/output_1"), commit12RunWorkflowActivity)).asJsonLD
      ) -> examplarData
    }
  }

  private val projectCreators: Gen[Person] = for {
    name  <- userNames
    email <- userEmails
  } yield Person(name, email)
}
