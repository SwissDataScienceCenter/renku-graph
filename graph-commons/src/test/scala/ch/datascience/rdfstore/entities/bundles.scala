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

import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.schemaVersions
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{listOf, nonEmptySet, setOf}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.{commitIds, committedDates}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PartLocation, PartName, PublishedDate, SameAs, Url}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.graph.model.{SchemaVersion, datasets, projects}
import ch.datascience.rdfstore.entities.CommandParameter._
import ch.datascience.rdfstore.entities.DataSetPart.{DataSetPartArtifact, dataSetParts}
import ch.datascience.rdfstore.entities.Location._
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.entities.RunPlan.Command
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
    val project = Project(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeParent)
    Activity(
      commitId,
      committedDate,
      committer,
      project,
      Agent(schemaVersion),
      maybeGenerationFactories = List(
        Generation.factory(
          DataSet.factory(
            datasetIdentifier,
            datasetName,
            maybeDatasetUrl,
            maybeDatasetSameAs,
            maybeDatasetDescription,
            maybeDatasetPublishedDate,
            datasetCreatedDate,
            datasetCreators,
            datasetParts.map {
              case (name, location) => DataSetPart.factory(name, location, None)(_)
            },
            Location(".renku") / "datasets" / datasetIdentifier
          )
        )
      )
    ).asJsonLD
  }

  object exemplarLineageFlow {

    final case class ExamplarData(
        location:          Location,
        commitId:          CommitId,
        `sha3 zhbikes`:    NodeDef,
        `sha7 plot_data`:  NodeDef,
        `sha7 clean_data`: NodeDef,
        `sha8 renku run`:  NodeDef,
        `sha8 parquet`:    NodeDef,
        `sha9 renku run`:  NodeDef,
        `sha9 plot_data`:  NodeDef,
        `sha9 cumulative`: NodeDef,
        `sha10 zhbikes`:   NodeDef,
        `sha12 parquet`:   NodeDef
    )

    final case class NodeDef(location: String, types: Set[String])

    object NodeDef {

      def apply(
          entity:              Entity with Artifact
      )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NodeDef = {
        val jsonLd = entity.asJsonLD
        NodeDef(
          entity.location.value,
          jsonLd.entityTypes
            .map(_.toList.map(_.toString))
            .getOrElse(throw new Exception("No entityTypes found"))
            .toSet
        )
      }

      def apply(entity: Activity)(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NodeDef = {
        val jsonLd = entity.asJsonLD
        NodeDef(
          entity.comment,
          jsonLd.entityTypes
            .map(_.toList.map(_.toString))
            .getOrElse(throw new Exception("No entityTypes found"))
            .toSet
        )
      }
    }

    def apply(
        projectPath:         Path = projectPaths.generateOne,
        schemaVersion:       SchemaVersion = schemaVersions.generateOne
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): (List[JsonLD], ExamplarData) = {
      val project =
        Project(projectPath, projectNames.generateOne, projectCreatedDates.generateOne, projectCreators.generateOption)
      val agent           = Agent(schemaVersion)
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
          entityFactory = DataSet.factory(
            id             = dataSetId,
            name           = datasets.Name("zhbikes"),
            createdDate    = datasetCreatedDates.generateOne,
            creators       = dataSetCreators,
            partsFactories = partsFactories,
            location       = Location(s".renku/datasets/$dataSetId")
          )
        )

      val commit2DataSetCreation = Activity(
        CommitId("000002"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment                  = "renku dataset create zhbikes",
        maybeGenerationFactories = List(dataSetGenerationFactory(partsFactories = Nil))
      )

      val commit3AddingDataSetFile = Activity(
        CommitId("000003"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit2DataSetCreation),
        maybeGenerationFactories = List(
          Generation.factory(
            entityFactory = Collection.factory(dataSetFolder, List(velo2019Location))
          )
        )
      )

      val commit4Activity = Activity(
        CommitId("000004"),
        committedDates.generateOne,
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
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "packages installed",
        maybeInformedBy = commit4Activity.some,
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(Location("requirements.txt")))
        )
      )

      val commit6Activity = Activity(
        CommitId("000006"),
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "added notebook",
        maybeInformedBy = commit5Activity.some,
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(Location("notebooks/zhbikes-notebook.ipynb")))
        )
      )

      val commit7Activity = Activity(
        CommitId("000007"),
        committedDates.generateOne,
        committer = persons.generateOne,
        project,
        agent,
        comment         = "added refactored scripts",
        maybeInformedBy = Some(commit6Activity),
        maybeGenerationFactories = List(
          Generation.factory(entityFactory = Entity.factory(plotData)),
          Generation.factory(entityFactory = Entity.factory(cleanData))
        )
      )

      val oldCommit8ProcessRun = Activity(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "1st-renku-run.cwl generation",
        maybeInformedBy = Some(commit7Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("1st-renku-run.cwl")))
        )
      )

      val commit8ParquetEntityFactory = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val commit8ProcessRun = ProcessRun.standAlone(
        CommitId("000008"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = s"renku run python $cleanData $dataSetFolder $bikesParquet",
        maybeInformedBy = Some(commit7Activity),
        associationFactory = Association.process(
          agent.copy(schemaVersion = schemaVersions.generateOne),
          RunPlan.process(
            WorkflowFile.yaml("1st-renku-run.yaml"),
            Command("python"),
            inputs = List(
              Input.from(commit7Activity.entity(cleanData)),
              Input.from(commit3AddingDataSetFile.entity(dataSetFolder))
            ),
            outputs = List(Output.from(commit8ParquetEntityFactory))
          )
        ),
        maybeInvalidation = oldCommit8ProcessRun.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val oldCommit9ProcessRun = Activity(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "2nd-renku-run.cwl generation",
        maybeInformedBy = Some(commit7Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("2nd-renku-run.cwl")))
        )
      )

      val commit9GridPlotEntityFactory = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      val commit9ProcessRun = ProcessRun.standAlone(
        CommitId("000009"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = s"renku run python $plotData $bikesParquet",
        maybeInformedBy = Some(commit8ProcessRun),
        associationFactory = Association.process(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan.process(
            WorkflowFile.yaml("2nd-renku-run.yaml"),
            Command("python"),
            inputs = List(Input.from(commit7Activity.entity(plotData)),
                          Input.from(commit8ProcessRun.processRunAssociation.runPlan.output(bikesParquet))),
            outputs = List(
              Output.from(activity => Entity(Generation(cumulativePng, activity))),
              Output.from(commit9GridPlotEntityFactory)
            )
          )
        ),
        maybeInvalidation = oldCommit9ProcessRun.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val commit10Activity = Activity(
        CommitId("0000010"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku dataset: committing 1 newly added files",
        maybeInformedBy = Some(commit9ProcessRun),
        maybeGenerationFactories = List(
          Generation.factory(Collection.factory(dataSetFolder, List(velo2019Location, velo2018Location)))
        )
      )

      val commit11Activity = Activity(
        CommitId("0000011"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku dataset add zhbikes velo.csv",
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
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku-update.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-update.cwl")))
        )
      )

      val oldCommit12WorkflowStep0 = Activity(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku-migrate-step0.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-migrate-step0.cwl")))
        )
      )

      val oldCommit12WorkflowStep1 = Activity(
        commitIds.generateOne,
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent,
        comment         = "renku-migrate-step1.cwl generation",
        maybeInformedBy = Some(commit11Activity),
        maybeGenerationFactories = List(
          Generation.factory(Entity.factory(WorkflowFile.cwl("renku-migrate-step1.cwl")))
        )
      )

      val commit12CommandPlotDataInput       = Input.from(commit7Activity.entity(plotData))
      val commit12CommandCleanDataInput      = Input.from(commit7Activity.entity(cleanData))
      val commit12CommandDataSetFolderInput  = Input.from(commit10Activity.entity(dataSetFolder))
      val commit12CumulativePngEntityFactory = (activity: Activity) => Entity(Generation(cumulativePng, activity))
      val commit12GridPlotPngEntityFactory   = (activity: Activity) => Entity(Generation(gridPlotPng, activity))
      val commit12ParquetEntityFactory       = (activity: Activity) => Entity(Generation(bikesParquet, activity))
      val commit12Workflow = WorkflowRun(
        CommitId("0000012"),
        committedDates.generateOne,
        persons.generateOne,
        project,
        agent.copy(maybeStartedBy = Some(persons.generateOne)),
        comment = "renku update",
        WorkflowFile.yaml("renku-update.yaml"),
        informedBy = commit11Activity,
        associationFactory = Association.workflow(
          agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne)),
          RunPlan.workflow(
            inputs = List(
              commit12CommandPlotDataInput,
              commit12CommandCleanDataInput,
              commit12CommandDataSetFolderInput
            ),
            outputs = List(
              Output.from(commit12CumulativePngEntityFactory),
              Output.from(commit12GridPlotPngEntityFactory),
              Output.from(commit12ParquetEntityFactory)
            ),
            subprocesses = List(
              RunPlan.child(
                WorkflowFile.yaml("renku-migrate-step0.yaml"),
                Command("python"),
                inputs  = List(commit12CommandCleanDataInput, commit12CommandDataSetFolderInput),
                outputs = List(Output.from(commit12ParquetEntityFactory))
              ),
              RunPlan.child(
                WorkflowFile.yaml("renku-migrate-step1.yaml"),
                Command("python"),
                inputs = List(commit12CommandPlotDataInput, Input.fromFactory(commit12ParquetEntityFactory)),
                outputs = List(
                  Output.from(commit12CumulativePngEntityFactory),
                  Output.from(commit12GridPlotPngEntityFactory)
                )
              )
            )
          )
        ),
        processRunsFactories = List(
          ProcessRun.child(
            associationFactory = Association.child(
              agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne))
            ),
            maybeInvalidation = oldCommit12WorkflowStep0.generations.headOption.flatMap(_.maybeReverseEntity)
          ),
          ProcessRun.child(
            associationFactory = Association.child(
              agent.copy(schemaVersion = schemaVersions.generateOne, maybeStartedBy = Some(persons.generateOne))
            ),
            maybeInvalidation = oldCommit12WorkflowStep1.generations.headOption.flatMap(_.maybeReverseEntity)
          )
        ),
        maybeInvalidation = oldCommit12Workflow.generations.headOption.flatMap(_.maybeReverseEntity)
      )

      val examplarData = ExamplarData(
        gridPlotPng,
        commit12Workflow.commitId,
        NodeDef(commit3AddingDataSetFile.entity(dataSetFolder)),
        NodeDef(commit7Activity.entity(plotData)),
        NodeDef(commit7Activity.entity(cleanData)),
        NodeDef(commit8ProcessRun),
        NodeDef(commit8ProcessRun.processRunAssociation.runPlan.output(bikesParquet)),
        NodeDef(commit9ProcessRun),
        NodeDef(commit9ProcessRun.processRunAssociation.runPlan.output(gridPlotPng)),
        NodeDef(commit9ProcessRun.processRunAssociation.runPlan.output(cumulativePng)),
        NodeDef(commit10Activity.entity(dataSetFolder)),
        NodeDef(commit12Workflow.processRunAssociation.runPlan.output(bikesParquet))
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
}
