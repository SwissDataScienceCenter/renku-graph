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

import cats.Show
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets
import ch.datascience.rdfstore.entities.CommandParameterBase.CommandInput._
import ch.datascience.rdfstore.entities.CommandParameterBase.CommandOutput.{LocationCommandOutput, MappedCommandOutput}
import ch.datascience.rdfstore.entities.CommandParameterBase._
import ch.datascience.rdfstore.entities.Entity.InputEntity
import ch.datascience.rdfstore.entities.ParameterValue.PathParameterValue.{InputParameterValue, OutputParameterValue}
import ch.datascience.rdfstore.entities.ParameterValue.VariableParameterValue
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.rdfstore.entities.RunPlan.{Command, CommandParameters}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.syntax.JsonEncoderOps

/**  ====================== Exemplar data visualization ======================
  * zhbikes folder   clean_data
  *           \      /
  *          run plan 1
  *               \
  *              bikesParquet   plot_data
  *                       \     /
  *                      run plan 2
  *                       /     \
  *                grid_plot   cumulative
  */
object LineageExemplarData {

  final case class ExemplarData(
      project:               Project[ForksCount],
      `zhbikes folder`:      NodeDef,
      `clean_data entity`:   NodeDef,
      `bikesparquet entity`: NodeDef,
      `plot_data entity`:    NodeDef,
      `grid_plot entity`:    NodeDef,
      `cumulative entity`:   NodeDef,
      `activity3 plan1`:     NodeDef,
      `activity3 date`:      Activity.StartTime,
      `activity4 plan2`:     NodeDef,
      `activity4 date`:      Activity.StartTime
  )

  def apply(
      project:             Project[ForksCount] = projectEntities[ForksCount.Zero](visibilityPublic).generateOne
  )(implicit renkuBaseUrl: RenkuBaseUrl): (List[JsonLD], ExemplarData) = {

    val zhbikesFolder = Location.Folder("data/zhbikes")
    val velo2018      = Location.File(zhbikesFolder, "2018velo.csv")
    val velo2019      = Location.File(zhbikesFolder, "2019velo.csv")
    val plotData      = Location.File("src/plot_data.py")
    val cleanData     = Location.File("src/clean_data.py")
    val bikesParquet  = Location.File("data/preprocessed/zhbikes.parquet")
    val cumulative    = Location.File("figs/cumulative.png")
    val gridPlot      = Location.File("figs/grid_plot.png")

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
        parts = List(datasetPart(velo2018), datasetPart(velo2019))
      )
    }

    val runPlan1 = RunPlan(
      RunPlan.Name("runPlan1"),
      Command("python"),
      CommandParameters.of(CommandInput.fromLocation(cleanData),
                           CommandInput.fromLocation(zhbikesFolder),
                           CommandOutput.fromLocation(bikesParquet)
      ),
      project
    )

    val activity1RunPlan1 = ExecutionPlanner
      .of(runPlan1,
          activityStartTimes(after = project.dateCreated).generateOne,
          personEntities.generateOne,
          project.agent
      )
      .planInputParameterValuesFromChecksum(
        cleanData     -> entityChecksums.generateOne,
        zhbikesFolder -> entityChecksums.generateOne
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val runPlan2 = RunPlan(
      RunPlan.Name("runPlan2"),
      Command("python"),
      CommandParameters.of(
        CommandInput.fromLocation(plotData),
        CommandInput.fromLocation(bikesParquet),
        CommandOutput.fromLocation(cumulative),
        CommandOutput.fromLocation(gridPlot)
      ),
      project
    )

    val activity2RunPlan2 = ExecutionPlanner
      .of(runPlan2,
          activityStartTimes(after = activity1RunPlan1.startTime).generateOne,
          personEntities.generateOne,
          project.agent
      )
      .planInputParameterValuesFromChecksum(
        plotData -> entityChecksums.generateOne
      )
      .planInputParameterValuesFromEntity(
        bikesParquet -> activity1RunPlan1
          .findGenerationEntity(bikesParquet)
          .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val activity3RunPlan1 = ExecutionPlanner
      .of(runPlan1,
          activityStartTimes(after = activity2RunPlan2.startTime).generateOne,
          personEntities.generateOne,
          project.agent
      )
      .planInputParameterValuesFromChecksum(
        cleanData -> activity1RunPlan1
          .findUsagesChecksum(cleanData)
          .getOrElse(throw new Exception(s"No usage for $cleanData")),
        zhbikesFolder -> entityChecksums.generateOne
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val activity4RunPlan2 = ExecutionPlanner
      .of(runPlan2,
          activityStartTimes(after = activity3RunPlan1.startTime).generateOne,
          personEntities.generateOne,
          project.agent
      )
      .planInputParameterValuesFromChecksum(
        plotData -> activity2RunPlan2
          .findUsagesChecksum(plotData)
          .getOrElse(throw new Exception(s"No usage found for $plotData"))
      )
      .planInputParameterValuesFromEntity(
        bikesParquet -> activity3RunPlan1
          .findGenerationEntity(bikesParquet)
          .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    List(
      zhbikesDataset.asJsonLD,
      runPlan1.asJsonLD,
      runPlan2.asJsonLD,
      activity1RunPlan1.asJsonLD,
      activity2RunPlan2.asJsonLD,
      activity3RunPlan1.asJsonLD,
      activity4RunPlan2.asJsonLD
    ) -> ExemplarData(
      project,
      NodeDef(activity3RunPlan1, zhbikesFolder),
      NodeDef(activity3RunPlan1, cleanData),
      NodeDef(activity3RunPlan1, bikesParquet),
      NodeDef(activity4RunPlan2, plotData),
      NodeDef(activity4RunPlan2, gridPlot),
      NodeDef(activity4RunPlan2, cumulative),
      NodeDef(activity3RunPlan1),
      activity3RunPlan1.startTime,
      NodeDef(activity4RunPlan2),
      activity4RunPlan2.startTime
    )
  }
}

final case class NodeDef(location: String, label: String, types: Set[String])

object NodeDef {

  def apply(activity: Activity, location: Location)(implicit renkuBaseUrl: RenkuBaseUrl): NodeDef =
    activity
      .findEntity(location)
      .map { entity =>
        NodeDef(
          entity.location.value,
          entity.location.value,
          entity.asJsonLD.entityTypes
            .map(_.toList.map(_.show))
            .getOrElse(throw new Exception("No entityTypes found"))
            .toSet
        )
      }
      .getOrElse(
        throw new Exception(
          s"No entity with $location on activity associated with ${activity.association.runPlan.name} plan"
        )
      )

  def apply(activity: Activity)(implicit renkuBaseUrl: RenkuBaseUrl): NodeDef =
    NodeDef(
      activity.association.runPlan.asJsonLD.entityId
        .getOrElse(throw new Exception("Non entity id found for Activity"))
        .toString,
      activity.show,
      activity.asJsonLD.entityTypes
        .map(_.toList.map(_.show))
        .getOrElse(throw new Exception("No entityTypes found"))
        .toSet
    )

  private implicit lazy val activityShow: Show[Activity] = Show.show { activity =>
    activity.parameters
      .sortBy(_.valueReference.position)
      .map(_.show)
      .mkString(s"${activity.association.runPlan.command} ", " ", "")
  }

  private implicit lazy val parameterValueShow: Show[ParameterValue] = Show.show {
    case value: VariableParameterValue => (value.valueReference -> value.value).show
    case value: InputParameterValue    => (value.valueReference -> value.location).show
    case value: OutputParameterValue   => (value.valueReference -> value.location).show
  }

  private implicit lazy val variableParameterValueShow: Show[(CommandParameter, VariableParameterValue.ValueOverride)] =
    Show.show { case (param, valueOverride) =>
      param.maybePrefix.fold(valueOverride.toString)(prefix => s"$prefix$valueOverride")
    }

  private implicit def pathParameterValueShow[P <: CommandInputOrOutput]: Show[(P, Location)] =
    Show.show {
      case (param: LocationCommandInput, location) =>
        param.maybePrefix.fold(location.toString)(prefix => s"$prefix$location")
      case (param: MappedCommandInput, location) =>
        param.maybePrefix.fold(s"${param.mappedTo.show} $location")(prefix =>
          s"$prefix ${param.mappedTo.show} $location"
        )
      case (param: LocationCommandOutput, location) =>
        param.maybePrefix.fold(location.toString)(prefix => s"$prefix$location")
      case (param: MappedCommandOutput, location) =>
        param.maybePrefix.fold(s"${param.mappedTo.show} $location")(prefix =>
          s"$prefix ${param.mappedTo.show} $location"
        )
    }

  private implicit def mappingShow[S <: IOStream]: Show[S] = Show.show {
    case _: IOStream.StdIn.type  => "<"
    case _: IOStream.StdOut.type => ">"
    case _: IOStream.StdErr.type => "2>"
  }
}
