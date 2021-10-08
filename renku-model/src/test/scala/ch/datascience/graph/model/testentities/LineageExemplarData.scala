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

package ch.datascience.graph.model.testentities

import cats.Show
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model._
import ch.datascience.graph.model.commandParameters.IOStream
import ch.datascience.graph.model.entityModel.{Location, LocationLike}
import ch.datascience.graph.model.plans.Command
import ch.datascience.graph.model.testentities.CommandParameterBase.CommandInput._
import ch.datascience.graph.model.testentities.CommandParameterBase.CommandOutput.{LocationCommandOutput, MappedCommandOutput}
import ch.datascience.graph.model.testentities.CommandParameterBase._
import ch.datascience.graph.model.testentities.Entity.InputEntity
import ch.datascience.graph.model.testentities.ParameterValue.CommandParameterValue
import ch.datascience.graph.model.testentities.ParameterValue.LocationParameterValue.{CommandInputValue, CommandOutputValue}
import ch.datascience.graph.model.testentities.Plan.CommandParameters
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
      project:               Project,
      `zhbikes folder`:      NodeDef,
      `clean_data entity`:   NodeDef,
      `bikesparquet entity`: NodeDef,
      `plot_data entity`:    NodeDef,
      `grid_plot entity`:    NodeDef,
      `cumulative entity`:   NodeDef,
      `activity3 node`:      NodeDef,
      `activity4 node`:      NodeDef,
      activity1:             Activity,
      activity2:             Activity,
      activity3:             Activity,
      activity4:             Activity
  )

  def apply(
      project:             Project = projectEntities(visibilityPublic, forksCountGen = anyForksCount).generateOne
  )(implicit renkuBaseUrl: RenkuBaseUrl): ExemplarData = {

    val zhbikesFolder = Location.Folder("data/zhbikes")
    val velo2018      = Location.File(zhbikesFolder, "2018velo.csv")
    val velo2019      = Location.File(zhbikesFolder, "2019velo.csv")
    val plotData      = Location.File("src/plot_data.py")
    val cleanData     = Location.File("src/clean_data.py")
    val bikesParquet  = Location.File("data/preprocessed/zhbikes.parquet")
    val cumulative    = Location.File("figs/cumulative.png")
    val gridPlot      = Location.File("figs/grid_plot.png")

    val zhbikesDataset = {
      val rawDataset = datasetEntities(provenanceInternal).toGeneratorFor(project).generateOne

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

    val plan1 = Plan(
      plans.Name("plan1"),
      Command("python"),
      CommandParameters.of(CommandInput.fromLocation(cleanData),
                           CommandInput.fromLocation(zhbikesFolder),
                           CommandOutput.fromLocation(bikesParquet)
      )
    )

    val activity1Plan1 = ExecutionPlanner
      .of(plan1, activityStartTimes(after = project.dateCreated).generateOne, personEntities.generateOne, project)
      .planInputParameterValuesFromChecksum(
        cleanData     -> entityChecksums.generateOne,
        zhbikesFolder -> entityChecksums.generateOne
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val plan2 = Plan(
      plans.Name("plan2"),
      Command("python"),
      CommandParameters.of(
        CommandInput.fromLocation(plotData),
        CommandInput.fromLocation(bikesParquet),
        CommandOutput.fromLocation(cumulative),
        CommandOutput.fromLocation(gridPlot)
      )
    )

    val activity2Plan2 = ExecutionPlanner
      .of(plan2, activityStartTimes(after = activity1Plan1.startTime).generateOne, personEntities.generateOne, project)
      .planInputParameterValuesFromChecksum(
        plotData -> entityChecksums.generateOne
      )
      .planInputParameterValuesFromEntity(
        bikesParquet -> activity1Plan1
          .findGenerationEntity(bikesParquet)
          .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val activity3Plan1 = ExecutionPlanner
      .of(plan1, activityStartTimes(after = activity2Plan2.startTime).generateOne, personEntities.generateOne, project)
      .planInputParameterValuesFromChecksum(
        cleanData -> activity1Plan1
          .findUsagesChecksum(cleanData)
          .getOrElse(throw new Exception(s"No usage for $cleanData")),
        zhbikesFolder -> entityChecksums.generateOne
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    val activity4Plan2 = ExecutionPlanner
      .of(plan2, activityStartTimes(after = activity3Plan1.startTime).generateOne, personEntities.generateOne, project)
      .planInputParameterValuesFromChecksum(
        plotData -> activity2Plan2
          .findUsagesChecksum(plotData)
          .getOrElse(throw new Exception(s"No usage found for $plotData"))
      )
      .planInputParameterValuesFromEntity(
        bikesParquet -> activity3Plan1
          .findGenerationEntity(bikesParquet)
          .getOrElse(throw new Exception(s"No generation for $bikesParquet"))
      )
      .buildProvenanceGraph
      .fold(errors => throw new Exception(errors.toList.mkString), identity)

    ExemplarData(
      project.addDatasets(zhbikesDataset).addActivities(activity1Plan1, activity2Plan2, activity3Plan1, activity4Plan2),
      NodeDef(activity3Plan1, zhbikesFolder),
      NodeDef(activity3Plan1, cleanData),
      NodeDef(activity3Plan1, bikesParquet),
      NodeDef(activity4Plan2, plotData),
      NodeDef(activity4Plan2, gridPlot),
      NodeDef(activity4Plan2, cumulative),
      NodeDef(activity3Plan1),
      NodeDef(activity4Plan2),
      activity1Plan1,
      activity2Plan2,
      activity3Plan1,
      activity4Plan2
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
          entity.asJsonLD.entityTypes.getOrElse(throw new Exception("No entityTypes found")).toList.map(_.show).toSet
        )
      }
      .getOrElse(
        throw new Exception(
          s"No entity with $location on activity associated with ${activity.association.plan.name} plan"
        )
      )

  def apply(activity: Activity)(implicit renkuBaseUrl: RenkuBaseUrl): NodeDef = NodeDef(
    activity.asJsonLD.entityId.getOrElse(throw new Exception("Non entity id found for Activity")).show,
    activity.show,
    activity.asJsonLD.entityTypes.getOrElse(throw new Exception("No entityTypes found")).toList.map(_.show).toSet
  )

  private implicit lazy val activityShow: Show[Activity] = Show.show { activity =>
    activity.parameters
      .collect {
        case parameter @ CommandInputValue(_, _, valueReference: ExplicitCommandParameter, _) =>
          parameter -> valueReference.position
        case parameter @ CommandOutputValue(_, _, valueReference: ExplicitCommandParameter, _) =>
          parameter -> valueReference.position
        case parameter @ CommandParameterValue(_, _, valueReference: ExplicitCommandParameter, _) =>
          parameter -> valueReference.position
      }
      .sortBy(_._2)
      .map(_._1.show)
      .mkString(s"${activity.association.plan.command} ", " ", "")
  }

  private implicit def parameterValueShow[P <: ParameterValue]: Show[P] = Show.show {
    case value: CommandParameterValue => (value.valueReference -> value.value).show
    case value: CommandInputValue     => (value.valueReference -> value.value).show
    case value: CommandOutputValue    => (value.valueReference -> value.value).show
  }

  private implicit lazy val variableParameterValueShow: Show[(CommandParameter, parameterValues.ValueOverride)] =
    Show.show { case (param, valueOverride) =>
      param.maybePrefix.fold(valueOverride.toString)(prefix => s"$prefix$valueOverride")
    }

  private implicit def commandParameterShow[P <: CommandInputOrOutput]: Show[(P, LocationLike)] =
    Show.show {
      case (param: LocationCommandInput, location) =>
        param.maybePrefix.fold(location.toString)(prefix => s"$prefix$location")
      case (param: MappedCommandInput, location) =>
        param.maybePrefix.fold(s"${param.mappedTo.show} $location")(prefix =>
          s"$prefix ${param.mappedTo.show} $location"
        )
      case (_: ImplicitCommandInput, _) => ""
      case (param: LocationCommandOutput, location) =>
        param.maybePrefix.fold(location.toString)(prefix => s"$prefix$location")
      case (param: MappedCommandOutput, location) =>
        param.maybePrefix.fold(s"${param.mappedTo.show} $location")(prefix =>
          s"$prefix ${param.mappedTo.show} $location"
        )
    }

  private implicit def mappingShow[S <: IOStream]: Show[S] = Show.show {
    case _: IOStream.StdIn  => "<"
    case _: IOStream.StdOut => ">"
    case _: IOStream.StdErr => "2>"
  }
}
