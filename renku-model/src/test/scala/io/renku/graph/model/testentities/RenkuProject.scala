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

package io.renku.graph.model.testentities

import cats.data.ValidatedNel
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects._
import io.renku.graph.model.testentities.RenkuProject.CreateCompositePlan
import io.renku.graph.model.testentities.generators.EntitiesGenerators.{CompositePlanGenFactory, DatasetGenFactory, ProjectBasedGenFactory}

sealed trait RenkuProject extends Project with RenkuProject.RenkuProjectAlg with Product with Serializable {
  val path:                 Path
  val name:                 Name
  val maybeDescription:     Option[Description]
  val agent:                CliVersion
  val dateCreated:          DateCreated
  val maybeCreator:         Option[Person]
  val visibility:           Visibility
  val forksCount:           ForksCount
  val keywords:             Set[Keyword]
  val members:              Set[Person]
  val version:              SchemaVersion
  val activities:           List[Activity]
  val datasets:             List[Dataset[Dataset.Provenance]]
  val unlinkedPlans:        List[StepPlan]
  val createCompositePlans: List[CreateCompositePlan]

  type ProjectType <: RenkuProject

  lazy val stepPlans: List[StepPlan] = activities.map(_.association.plan) ::: unlinkedPlans

  lazy val plans: List[Plan] = stepPlans ::: createCompositePlans.flatMap(_.apply(stepPlans, dateCreated))

  lazy val compositePlans: List[CompositePlan] = plans.collect { case cp: CompositePlan => cp }
}

object RenkuProject {

  type CreateCompositePlan = (List[StepPlan], DateCreated) => Option[CompositePlan]

  /** For more convenient creation given the composite plan generator. Example for add a composite plan referencing 
   * all step plans:
   * {{{
   * p.addCompositePlan(CreateCompositePlan(compositePlanEntities))
   * }}}
   */
  object CreateCompositePlan {
    def apply(f: ProjectBasedGenFactory[List[Plan]] => CompositePlanGenFactory): CreateCompositePlan =
      (plans, created) => Option.when(plans.nonEmpty)(f(ProjectBasedGenFactory.pure(plans)).run(created).generateOne)
  }

  final case class WithoutParent(path:                 Path,
                                 name:                 Name,
                                 maybeDescription:     Option[Description],
                                 agent:                CliVersion,
                                 dateCreated:          DateCreated,
                                 maybeCreator:         Option[Person],
                                 visibility:           Visibility,
                                 forksCount:           ForksCount,
                                 keywords:             Set[Keyword],
                                 members:              Set[Person],
                                 version:              SchemaVersion,
                                 activities:           List[Activity],
                                 datasets:             List[Dataset[Dataset.Provenance]],
                                 images:               List[ImageUri],
                                 unlinkedPlans:        List[StepPlan] = List.empty,
                                 createCompositePlans: List[CreateCompositePlan] = List.empty
  ) extends RenkuProject {

    validateDates(dateCreated, activities, datasets)
      .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

    override type ProjectType = RenkuProject.WithoutParent

    override def addActivities(toAdd: Activity*): RenkuProject.WithoutParent =
      copy(activities = activities ::: toAdd.toList)

    override def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): RenkuProject.WithoutParent =
      copy(datasets = datasets ::: toAdd.toList)

    override def addDataset[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): (Dataset[P], RenkuProject.WithoutParent) = {
      val dataset = factory(dateCreated).generateOne
      dataset -> copy(datasets = datasets ::: dataset :: Nil)
    }

    override def addUnlinkedStepPlan(plan: StepPlan): ProjectType =
      copy(unlinkedPlans = plan :: unlinkedPlans)

    override def addCompositePlan(cp: CreateCompositePlan): WithoutParent =
      copy(createCompositePlans = cp :: createCompositePlans)

    override def replaceDatasets(newDatasets: Dataset[Dataset.Provenance]*): RenkuProject.WithoutParent =
      copy(datasets = newDatasets.toList)

    override def replaceActivities(newActivities: Activity*): RenkuProject.WithoutParent =
      copy(activities = newActivities.toList)

    private def validateDates(projectDateCreated: projects.DateCreated,
                              activities:         List[Activity],
                              datasets:           List[Dataset[Dataset.Provenance]]
    ): ValidatedNel[String, Unit] = activities
      .map { activity =>
        import activity._
        if ((startTime.value compareTo projectDateCreated.value) >= 0) ().validNel[String]
        else s"Activity $id startTime $startTime is older than project $projectDateCreated".invalidNel
      }
      .sequence
      .void |+| datasets
      .map {
        def compareDateWithProject(dataset: Dataset[Dataset.Provenance], dateCreated: model.datasets.DateCreated) =
          if ((dateCreated compareTo projectDateCreated.value) >= 0) ().validNel[String]
          else
            s"Dataset ${dataset.identification.identifier} startTime $dateCreated is older than project $projectDateCreated".invalidNel

        dataset =>
          dataset.provenance match {
            case p: Dataset.Provenance.Internal => compareDateWithProject(dataset, p.date)
            case p: Dataset.Provenance.Modified => compareDateWithProject(dataset, p.date)
            case _ => ().validNel[String]
          }
      }
      .sequence
      .void
  }

  final case class WithParent(path:                 Path,
                              name:                 Name,
                              maybeDescription:     Option[Description],
                              agent:                CliVersion,
                              dateCreated:          DateCreated,
                              maybeCreator:         Option[Person],
                              visibility:           Visibility,
                              forksCount:           ForksCount,
                              keywords:             Set[Keyword],
                              members:              Set[Person],
                              version:              SchemaVersion,
                              activities:           List[Activity],
                              datasets:             List[Dataset[Dataset.Provenance]],
                              parent:               RenkuProject,
                              images:               List[ImageUri],
                              unlinkedPlans:        List[StepPlan] = Nil,
                              createCompositePlans: List[CreateCompositePlan] = Nil
  ) extends RenkuProject
      with Parent {
    override type ProjectType = RenkuProject.WithParent

    override def addActivities(toAdd: Activity*): RenkuProject.WithParent =
      copy(activities = activities ::: toAdd.toList)

    override def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): RenkuProject.WithParent =
      copy(datasets = datasets ::: toAdd.toList)

    override def addDataset[P <: Dataset.Provenance](
        factory: DatasetGenFactory[P]
    ): (Dataset[P], RenkuProject.WithParent) = {
      val dataset = factory(dateCreated).generateOne
      dataset -> copy(datasets = datasets ::: dataset :: Nil)
    }

    override def addUnlinkedStepPlan(plan: StepPlan): ProjectType = copy(unlinkedPlans = plan :: unlinkedPlans)

    override def addCompositePlan(cp: CreateCompositePlan): WithParent =
      copy(createCompositePlans = cp :: createCompositePlans)

    override def replaceDatasets(newDatasets: Dataset[Dataset.Provenance]*): RenkuProject.WithParent =
      copy(datasets = newDatasets.toList)

    override def replaceActivities(newActivities: Activity*): RenkuProject.WithParent =
      copy(activities = newActivities.toList)
  }

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  trait RenkuProjectAlg {
    self: RenkuProject =>

    lazy val topAncestorDateCreated: DateCreated = this match {
      case project: RenkuProject.WithParent => project.parent.topAncestorDateCreated
      case project => project.dateCreated
    }

    def addActivities(toAdd: Activity*): ProjectType

    def addDatasets[P <: Dataset.Provenance](toAdd: Dataset[P]*): ProjectType

    def addDataset[P <: Dataset.Provenance](toAdd: DatasetGenFactory[P]): (Dataset[P], ProjectType)

    def addUnlinkedStepPlan(plan: StepPlan): ProjectType

    def addCompositePlan(cp: CreateCompositePlan): ProjectType

    def replaceDatasets(newDatasets: Dataset[Dataset.Provenance]*): ProjectType

    def replaceActivities(newActivities: Activity*): ProjectType
  }

  implicit def toEntitiesRenkuProject(implicit renkuUrl: RenkuUrl): RenkuProject => entities.RenkuProject = {
    case p: RenkuProject.WithoutParent => toEntitiesRenkuProjectWithoutParent(renkuUrl)(p)
    case p: RenkuProject.WithParent    => toEntitiesRenkuProjectWithParent(renkuUrl)(p)
  }

  implicit def toEntitiesRenkuProjectWithoutParent(implicit
      renkuUrl: RenkuUrl
  ): RenkuProject.WithoutParent => entities.RenkuProject.WithoutParent =
    project =>
      entities.RenkuProject.WithoutParent
        .from(
          projects.ResourceId(project.asEntityId),
          project.path,
          project.name,
          project.maybeDescription,
          project.agent,
          project.dateCreated,
          project.maybeCreator.map(_.to[entities.Person]),
          project.visibility,
          project.keywords,
          project.members.map(_.to[entities.Person]),
          project.version,
          project.activities.map(_.to[entities.Activity]),
          project.datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          project.plans.map(_.to[entities.Plan]),
          convertImageUris(project.asEntityId)(project.images)
        )
        .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  implicit def toEntitiesRenkuProjectWithParent(implicit
      renkuUrl: RenkuUrl
  ): RenkuProject.WithParent => entities.RenkuProject.WithParent =
    project =>
      entities.RenkuProject.WithParent
        .from(
          projects.ResourceId(project.asEntityId),
          project.path,
          project.name,
          project.maybeDescription,
          project.agent,
          project.dateCreated,
          project.maybeCreator.map(_.to[entities.Person]),
          project.visibility,
          project.keywords,
          project.members.map(_.to[entities.Person]),
          project.version,
          project.activities.map(_.to[entities.Activity]),
          project.datasets.map(_.to[entities.Dataset[entities.Dataset.Provenance]]),
          project.plans.map(_.to[entities.Plan]),
          projects.ResourceId(project.parent.asEntityId),
          convertImageUris(project.asEntityId)(project.images)
        )
        .fold(errors => throw new IllegalStateException(errors.intercalate("; ")), identity)

  implicit def encoder[P <: RenkuProject](implicit
      renkuUrl:     RenkuUrl,
      gitLabApiUrl: GitLabApiUrl,
      graph:        GraphClass
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: RenkuProject.WithParent    => project.to[entities.RenkuProject.WithParent].asJsonLD
    case project: RenkuProject.WithoutParent => project.to[entities.RenkuProject.WithoutParent].asJsonLD
  }
}
