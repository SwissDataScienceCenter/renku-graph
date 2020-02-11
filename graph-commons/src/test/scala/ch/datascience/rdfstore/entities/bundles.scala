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

import ch.datascience.generators.CommonGraphGenerators.{emails, names, schemaVersions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{listOf, setOf}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PartLocation, PartName, PublishedDate, SameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, FilePath, ProjectPath}
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
      committer:     Person        = Person(names.generateOne, emails.generateOne),
      schemaVersion: SchemaVersion = schemaVersions.generateOne
  )(
      projectPath:         ProjectPath = projectPaths.generateOne,
      projectName:         projects.Name = projectNames.generateOne,
      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
      projectCreator:      Person = committer
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    ArtifactEntity(
      Generation(filePath,
                 Activity(
                   commitId,
                   committedDate,
                   committer,
                   Project(projectPath, projectName, projectDateCreated, projectCreator),
                   Agent(schemaVersion)
                 ))
    ).asJsonLD

  def randomDataSetCommit(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
    dataSetCommit()()()

  def dataSetCommit(
      commitId:      CommitId      = commitIds.generateOne,
      committedDate: CommittedDate = committedDates.generateOne,
      committer:     Person        = Person(names.generateOne, emails.generateOne),
      schemaVersion: SchemaVersion = schemaVersions.generateOne
  )(
      projectPath:        ProjectPath          = projectPaths.generateOne,
      projectName:        projects.Name        = projectNames.generateOne,
      projectDateCreated: projects.DateCreated = DateCreated(committedDate.value),
      projectCreator:     Person               = committer
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
    val project: Project = Project(projectPath, projectName, projectDateCreated, projectCreator)
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
    // format: off
    final case class ExamplarData(
        commitId: CommitId,
        filePath: FilePath,
        `sha7 plot_data`: NodeDef           = NodeDef(name = "/blob/000007/src/plot_data.py",                   label = "src/plot_data.py@000007"),
        `sha7 clean_data`: NodeDef          = NodeDef(name = "/blob/000007/src/clean_data.py",                  label = "src/clean_data.py@000007"),
        `sha8 renku run`: NodeDef           = NodeDef(name = "/commit/000008",                                  label = "renku run python"),
        `sha9 renku run`: NodeDef           = NodeDef(name = "/commit/000009",                                  label = "renku run python"),
        `sha10 zhbikes`: NodeDef            = NodeDef(name = "/blob/0000010/data/zhbikes",                      label = "data/zhbikes@0000010"),
        `sha12 step1 renku update`: NodeDef = NodeDef(name = "/commit/0000012/steps/step_1",                    label = "renku update"),
        `sha12 step2 renku update`: NodeDef = NodeDef(name = "/commit/0000012/steps/step_2",                    label = "renku update"),
        `sha12 step2 grid_plot`: NodeDef    = NodeDef(name = "/blob/0000012/figs/grid_plot.png",                label = "figs/grid_plot.png@0000012"),
        `sha12 parquet`: NodeDef            = NodeDef(name = "/blob/0000012/data/preprocessed/zhbikes.parquet", label = "data/preprocessed/zhbikes.parquet@0000012")
    )
    case class NodeDef(name: String, label: String)
    // format: on

    def apply(
        projectPath:         ProjectPath = projectPaths.generateOne,
        commitId:            CommitId = CommitId("0000012"),
        filePath:            FilePath = FilePath("figs/grid_plot.png"),
        schemaVersion:       SchemaVersion = schemaVersions.generateOne
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): (List[JsonLD], ExamplarData) = {

      val projectCreator = Person(names.generateOne, emails.generateOne)
      val project        = Project(projectPath, projectNames.generateOne, projectCreatedDates.generateOne, projectCreator)
      val agent          = Agent(schemaVersion)
      val dataSetId      = datasets.Identifier("d67a1653-0b6e-463b-89a0-afe72a53c8bb")
      val examplarData   = ExamplarData(commitId, filePath)

      val commit3Id  = CommitId("000003")
      val commit7Id  = CommitId("000007")
      val commit8Id  = CommitId("000008")
      val commit9Id  = CommitId("000009")
      val commit10Id = CommitId("0000010")
      val outFile1   = FilePath("figs/cumulative.png")
      val commit12Id = examplarData.commitId
      val outFile2   = examplarData.filePath
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
          Usage(commit8Id,
                FilePath("inputs/input_1"),
                ArtifactEntity(commit7Id, FilePath("src/clean_data.py"), project)),
          Usage(
            commit8Id,
            FilePath("inputs/input_2"),
            ArtifactEntityCollection(commit3Id,
                                     FilePath("data/zhbikes"),
                                     project,
                                     members =
                                       List(ArtifactEntity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project)))
          )
        )
      )
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
          Usage(commit9Id,
                FilePath("inputs/input_1"),
                ArtifactEntity(commit7Id, FilePath("src/plot_data.py"), project)),
          Usage(commit9Id,
                FilePath("inputs/input_2"),
                ArtifactEntity(commit8Id, FilePath("data/preprocessed/zhbikes.parquet"), project))
        )
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
      lazy val commit11Activity = Activity(
        CommitId("0000011"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku dataset add zhbikes velo.csv",
        maybeInformedBy = Some(commit10Activity)
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
          Usage(commit12Id,
                FilePath("inputs/input_1"),
                ArtifactEntity(commit7Id, FilePath("src/plot_data.py"), project)),
          Usage(commit12Id,
                FilePath("inputs/input_2"),
                ArtifactEntity(commit7Id, FilePath("src/clean_data.py"), project)),
          Usage(
            commit12Id,
            FilePath("inputs/input_3"),
            ArtifactEntityCollection(
              commit10Id,
              FilePath("data/zhbikes"),
              project,
              members = List(ArtifactEntity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project),
                             ArtifactEntity(commit10Id, FilePath("data/zhbikes/2018velo.csv"), project))
            )
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
          Usage(commit12Id,
                FilePath("steps/step_1/inputs/input_1"),
                ArtifactEntity(commit7Id, FilePath("src/plot_data.py"), project)),
          Usage(commit12Id,
                FilePath("steps/step_1/inputs/input_2"),
                ArtifactEntity(commit12Id, FilePath("data/preprocessed/zhbikes.parquet"), project))
        ),
        maybeStepId               = Some("steps/step_1"),
        maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
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
        ArtifactEntity(outFile2, Generation(FilePath("outputs/output_1"), commit9ProcessRunActivity)).asJsonLD,
        ArtifactEntity(Generation(FilePath("data/zhbikes/2018velo.csv"), commit10Activity)).asJsonLD,
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
        ArtifactEntity(
          FilePath("data/preprocessed/zhbikes.parquet"),
          Generation(
            FilePath("steps/step_2/outputs/output_0"),
            ProcessRunActivity(
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
                Usage(commit12Id,
                      FilePath("steps/step_2/inputs/input_1"),
                      ArtifactEntity(commit7Id, FilePath("src/clean_data.py"), project)),
                Usage(
                  commit12Id,
                  FilePath("steps/step_2/inputs/input_2"),
                  ArtifactEntityCollection(
                    commit10Id,
                    FilePath("data/zhbikes"),
                    project,
                    members = List(ArtifactEntity(commit3Id, FilePath("data/zhbikes/2019velo.csv"), project),
                                   ArtifactEntity(commit10Id, FilePath("data/zhbikes/2018velo.csv"), project))
                  )
                )
              ),
              maybeStepId               = Some("steps/step_2"),
              maybeWasPartOfWorkflowRun = Some(commit12RunWorkflowActivity)
            )
          )
        ).asJsonLD,
        ArtifactEntity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12Step1ProcessRunActivity)).asJsonLD,
        ArtifactEntity(outFile2, Generation(FilePath("steps/step_1/outputs/output_1"), commit12Step1ProcessRunActivity)).asJsonLD,
        ArtifactEntity(FilePath("data/preprocessed/zhbikes.parquet"),
                       Generation(FilePath("steps/step_2/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        ArtifactEntity(outFile1, Generation(FilePath("steps/step_1/outputs/output_0"), commit12RunWorkflowActivity)).asJsonLD,
        ArtifactEntity(outFile2, Generation(FilePath("steps/step_1/outputs/output_1"), commit12RunWorkflowActivity)).asJsonLD
      ) -> examplarData
    }
  }
}
