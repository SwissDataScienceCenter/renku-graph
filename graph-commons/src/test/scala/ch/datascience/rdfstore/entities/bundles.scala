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

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.Schemas
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{Path, Visibility}
import ch.datascience.graph.model.{CliVersion, datasets}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter, IOStream, Position}
import ch.datascience.rdfstore.entities.Entity.InputEntity
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.rdfstore.entities.RunPlan.{Command, CommandParameters}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax.JsonEncoderOps

import java.time.Instant
import scala.jdk.CollectionConverters._

object bundles extends Schemas {

//  implicit lazy val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl("https://dev.renku.ch")

//  def randomDataSetActivity: Activity =
//    Gen.oneOf(nonModifiedDataSetActivity()()(), modifiedDataSetActivity()()()).generateOne
//
//  def nonModifiedDataSetCommit(
//      commitId:      CommitId = commitIds.generateOne,
//      committedDate: CommittedDate = committedDates.generateOne,
//      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
//      cliVersion:    CliVersion = cliVersions.generateOne
//  )(
//      projectPath:         Path = projectPaths.generateOne,
//      projectName:         projects.Name = projectNames.generateOne,
//      maybeVisibility:     Option[Visibility] = None,
//      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
//      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
//      maybeParent:         Option[Project] = None,
//      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
//  )(
//      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
//      datasetTitle:               Title = datasetTitles.generateOne,
//      datasetName:                Name = datasetNames.generateOne,
//      datasetUrl:                 Url = datasetUrls.generateOne,
//      maybeDatasetSameAs:         Option[SameAs] = Gen.option(datasetSameAs).generateOne,
//      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
//      dates:                      Dates = datasetDates.generateOne,
//      datasetCreators:            Set[Person] = setOf(persons).generateOne,
//      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
//      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
//      datasetImages:              List[ImageUri] = listOf(datasetImageUris).generateOne,
//      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
//      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
//  )(implicit renkuBaseUrl:        RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
//    nonModifiedDataSetActivity(
//      commitId,
//      committedDate,
//      committer,
//      cliVersion
//    )(projectPath, projectName, maybeVisibility, projectDateCreated, maybeProjectCreator, maybeParent, projectVersion)(
//      datasetIdentifier,
//      datasetTitle,
//      datasetName,
//      datasetUrl,
//      maybeDatasetSameAs,
//      maybeDatasetDescription,
//      dates,
//      datasetCreators,
//      datasetParts,
//      datasetKeywords,
//      datasetImages,
//      overrideTopmostSameAs,
//      overrideTopmostDerivedFrom
//    ).asJsonLD
//
//  def nonModifiedDataSetActivity(
//      commitId:      CommitId = commitIds.generateOne,
//      committedDate: CommittedDate = committedDates.generateOne,
//      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
//      cliVersion:    CliVersion = cliVersions.generateOne
//  )(
//      projectPath:         Path = projectPaths.generateOne,
//      projectName:         projects.Name = projectNames.generateOne,
//      maybeVisibility:     Option[Visibility] = None,
//      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
//      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
//      maybeParent:         Option[Project] = None,
//      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
//  )(
//      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
//      datasetTitle:               Title = datasetTitles.generateOne,
//      datasetName:                Name = datasetNames.generateOne,
//      datasetUrl:                 Url = datasetUrls.generateOne,
//      maybeDatasetSameAs:         Option[SameAs] = Gen.option(datasetSameAs).generateOne,
//      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
//      dates:                      Dates = datasetDates.generateOne,
//      datasetCreators:            Set[Person] = setOf(persons).generateOne,
//      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
//      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
//      datasetImages:              List[ImageUri] = listOf(datasetImageUris).generateOne,
//      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
//      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
//  ): Activity = Activity(
//    commitId,
//    committedDate,
//    committer,
//    Project(projectPath,
//            projectName,
//            projectDateCreated,
//            maybeProjectCreator,
//            maybeVisibility,
//            maybeParentProject = maybeParent,
//            version = projectVersion
//    ),
//    Agent(cliVersion),
//    maybeGenerationFactories = List(
//      Generation.factory(
//        DataSet.nonModifiedFactory(
//          datasetIdentifier,
//          datasetTitle,
//          datasetName,
//          datasetUrl,
//          maybeDatasetSameAs,
//          maybeDatasetDescription,
//          dates,
//          datasetCreators,
//          datasetParts.map { case (name, location) =>
//            DataSetPart.factory(name, location, None)(_)
//          },
//          datasetKeywords,
//          datasetImages,
//          overrideTopmostSameAs = overrideTopmostSameAs,
//          overrideTopmostDerivedFrom = overrideTopmostDerivedFrom
//        )
//      )
//    )
//  )
//
//  def modifiedDataSetCommit(
//      commitId:      CommitId = commitIds.generateOne,
//      committedDate: CommittedDate = committedDates.generateOne,
//      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
//      cliVersion:    CliVersion = cliVersions.generateOne
//  )(
//      projectPath:         Path = projectPaths.generateOne,
//      projectName:         projects.Name = projectNames.generateOne,
//      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
//      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
//      maybeVisibility:     Option[Visibility] = None,
//      maybeParent:         Option[Project] = None,
//      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
//  )(
//      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
//      datasetTitle:               Title = datasetTitles.generateOne,
//      datasetName:                Name = datasetNames.generateOne,
//      datasetUrl:                 Url = datasetUrls.generateOne,
//      datasetDerivedFrom:         DerivedFrom = datasetDerivedFroms.generateOne,
//      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
//      dates:                      Dates = datasetDates.generateOne,
//      datasetCreators:            Set[Person] = setOf(persons).generateOne,
//      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
//      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
//      datasetImages:              List[ImageUri] = listOf(datasetImageUris).generateOne,
//      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
//      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
//  )(implicit renkuBaseUrl:        RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLD =
//    modifiedDataSetActivity(
//      commitId,
//      committedDate,
//      committer,
//      cliVersion
//    )(projectPath, projectName, projectDateCreated, maybeProjectCreator, maybeVisibility, maybeParent, projectVersion)(
//      datasetIdentifier,
//      datasetTitle,
//      datasetName,
//      datasetUrl,
//      datasetDerivedFrom,
//      maybeDatasetDescription,
//      dates,
//      datasetCreators,
//      datasetParts,
//      datasetKeywords,
//      datasetImages,
//      overrideTopmostSameAs,
//      overrideTopmostDerivedFrom
//    ).asJsonLD
//
//  def modifiedDataSetActivity(
//      commitId:      CommitId = commitIds.generateOne,
//      committedDate: CommittedDate = committedDates.generateOne,
//      committer:     Person = Person(userNames.generateOne, userEmails.generateOne),
//      cliVersion:    CliVersion = cliVersions.generateOne
//  )(
//      projectPath:         Path = projectPaths.generateOne,
//      projectName:         projects.Name = projectNames.generateOne,
//      projectDateCreated:  projects.DateCreated = DateCreated(committedDate.value),
//      maybeProjectCreator: Option[Person] = projectCreators.generateOption,
//      maybeVisibility:     Option[Visibility] = None,
//      maybeParent:         Option[Project] = None,
//      projectVersion:      SchemaVersion = projectSchemaVersions.generateOne
//  )(
//      datasetIdentifier:          Identifier = datasetIdentifiers.generateOne,
//      datasetTitle:               Title = datasetTitles.generateOne,
//      datasetName:                Name = datasetNames.generateOne,
//      datasetUrl:                 Url = datasetUrls.generateOne,
//      datasetDerivedFrom:         DerivedFrom = datasetDerivedFroms.generateOne,
//      maybeDatasetDescription:    Option[Description] = Gen.option(datasetDescriptions).generateOne,
//      dates:                      Dates = datasetDates.generateOne,
//      datasetCreators:            Set[Person] = setOf(persons).generateOne,
//      datasetParts:               List[(PartName, PartLocation)] = listOf(dataSetParts).generateOne,
//      datasetKeywords:            List[Keyword] = listOf(GraphModelGenerators.datasetKeywords).generateOne,
//      datasetImages:              List[ImageUri] = listOf(datasetImageUris).generateOne,
//      overrideTopmostSameAs:      Option[TopmostSameAs] = None,
//      overrideTopmostDerivedFrom: Option[TopmostDerivedFrom] = None
//  ): Activity = Activity(
//    commitId,
//    committedDate,
//    committer,
//    Project(projectPath,
//            projectName,
//            projectDateCreated,
//            maybeProjectCreator,
//            maybeVisibility,
//            maybeParentProject = maybeParent,
//            version = projectVersion
//    ),
//    Agent(cliVersion),
//    maybeGenerationFactories = List(
//      Generation.factory(
//        DataSet.modifiedFactory(
//          datasetIdentifier,
//          datasetTitle,
//          datasetName,
//          datasetUrl,
//          datasetDerivedFrom,
//          maybeDatasetDescription,
//          dates,
//          datasetCreators,
//          datasetParts.map { case (name, location) =>
//            DataSetPart.factory(name, location, None)(_)
//          },
//          datasetKeywords,
//          datasetImages,
//          overrideTopmostSameAs = overrideTopmostSameAs,
//          overrideTopmostDerivedFrom = overrideTopmostDerivedFrom
//        )
//      )
//    )
//  )
//
  object exemplarLineageFlow {

    final case class ExemplarData(
        location:              Location,
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
        projectVisibility:   Visibility = projectVisibilities.generateOne,
        projectMembers:      Set[Person] = Set.empty[Person]
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): (List[JsonLD], ExemplarData) = {
      val project =
        projectEntities[ForksCount.Zero](fixed(projectVisibility)).map(_.copy(agent = cliVersion)).generateOne

      val dataSetFolder    = Location("data/zhbikes")
      val plotData         = Location("src/plot_data.py")
      val cleanData        = Location("src/clean_data.py")
      val bikesParquet     = Location("data/preprocessed/zhbikes.parquet")
      val cumulativePng    = Location("figs/cumulative.png")
      val gridPlotPng      = Location("figs/grid_plot.png")
      val velo2018Location = Location("data/zhbikes/2018velo.csv")
      val velo2019Location = Location("data/zhbikes/2019velo.csv")

      val zhbikesDataset = {
        val rawDataset = datasetEntities(datasetProvenanceInternal).generateOne

        def datasetPart(location: Location) = {
          val rawPart = datasetPartEntities(rawDataset.provenance.date.instant).generateOne
          rawPart.copy(entity = rawPart.entity match {
            case e: InputEntity => e.copy(location = location)
            case _ => throw new Exception("Part should have an InputEntity")
          })
        }

        rawDataset.copy(
          identification =
            rawDataset.identification.copy(title = datasets.Title("zhbikes"), name = datasets.Name("zhbikes")),
          parts = List(datasetPart(velo2018Location), datasetPart(velo2019Location))
        )
      }

      val runPlan1 = RunPlan(
        runPlanNames.generateOne,
        Command("python"),
        CommandParameters.of(CommandInput.fromLocation(cleanData),
                             CommandInput.fromLocation(dataSetFolder),
                             CommandOutput.fromLocation(bikesParquet)
        )
      )

      val activity1RunPlan1 = ExecutionPlanner
        .of(runPlan1, activityStartTimes.generateOne, persons.generateOne, project.agent, project)
        .planParameterInputsValues(
          cleanData     -> entityChecksums.generateOne,
          dataSetFolder -> entityChecksums.generateOne
        )
        .buildProvenanceGraph
        .fold(errors => throw new Exception(errors.toList.mkString), identity)

      val runPlan2 = RunPlan(
        runPlanNames.generateOne,
        Command("python"),
        CommandParameters.of(
          CommandInput.fromLocation(plotData),
          CommandInput.fromLocation(bikesParquet),
          CommandOutput.fromLocation(cumulativePng),
          CommandOutput.fromLocation(gridPlotPng)
        )
      )

      val activity2RunPlan2 = ExecutionPlanner
        .of(runPlan1, activityStartTimes.generateOne, persons.generateOne, project.agent, project)
        .planParameterInputsValues(
          plotData -> entityChecksums.generateOne,
          bikesParquet -> activity1RunPlan1
            .findGenerationChecksum(bikesParquet)
            .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
        )
        .buildProvenanceGraph
        .fold(errors => throw new Exception(errors.toList.mkString), identity)

      val activity3RunPlan1 = ExecutionPlanner
        .of(runPlan1, activityStartTimes.generateOne, persons.generateOne, project.agent, project)
        .planParameterInputsValues(
          cleanData     -> activity1RunPlan1.findUsagesChecksum(cleanData).getOrElse(s"No usage for $cleanData"),
          dataSetFolder -> entityChecksums.generateOne
        )
        .buildProvenanceGraph
        .fold(errors => throw new Exception(errors.toList.mkString), identity)

      val activity4RunPlan2 = ExecutionPlanner
        .of(runPlan1, activityStartTimes.generateOne, persons.generateOne, project.agent, project)
        .planParameterInputsValues(
          plotData -> activity2RunPlan2
            .findUsagesChecksum(plotData)
            .getOrElse(throw new Exception(s"No usage found for $plotData")),
          bikesParquet -> activity3RunPlan1
            .findGenerationChecksum(bikesParquet)
            .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
        )
        .buildProvenanceGraph
        .fold(errors => throw new Exception(errors.toList.mkString), identity)

      val examplarData = ExemplarData(
        gridPlotPng,
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

  final case class NodeDef(location: String, label: String, types: Set[String])

  object NodeDef {

    def apply(
        entity:              Entity
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
        activity:            Activity
    )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): NodeDef =
      NodeDef(
        activity.association.runPlan.asJsonLD.entityId
          .getOrElse(throw new Exception("Non entity id found for ProcessRun"))
          .toString,
        activity.association.runPlan.asLabel,
        activity.asJsonLD.entityTypes
          .map(_.toList.map(_.toString))
          .getOrElse(throw new Exception("No entityTypes found"))
          .toSet
      )

    private implicit class RunPlanOps(runPlan: RunPlan) {

      lazy val asLabel: String =
        (runPlan.parameters ++ runPlan.inputs ++ runPlan.outputs)
          .sortBy(_.position)
          .foldLeft(List.empty[(Position, String)].asJava) {
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
