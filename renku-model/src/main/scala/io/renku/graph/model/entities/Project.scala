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

package io.renku.graph.model.entities

import PlanLens.{getPlanDerivation, setPlanDerivation}
import cats.Show
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.cli.model.{CliPerson, CliProject}
import io.renku.graph.model._
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.entities.RenkuProject.ProjectFactory
import io.renku.graph.model.images.{Image, ImageUri}
import io.renku.graph.model.projects._
import io.renku.graph.model.versions.{CliVersion, SchemaVersion}
import io.renku.jsonld.Property
import io.renku.jsonld.ontology._
import io.renku.tinytypes.InstantTinyType
import monocle.{Lens, Traversal}

import Ordered.orderingToOrdered

sealed trait Project extends Product with Serializable {
  val resourceId:       ResourceId
  val path:             Path
  val name:             Name
  val maybeDescription: Option[Description]
  val dateCreated:      DateCreated
  val dateModified:     DateModified
  val maybeCreator:     Option[Person]
  val visibility:       Visibility
  val keywords:         Set[Keyword]
  val members:          Set[Person]
  val images:           List[Image]

  val activities: List[Activity]
  val datasets:   List[Dataset[Dataset.Provenance]]
  val plans:      List[Plan]
  lazy val namespaces:     List[Namespace]       = path.toNamespaces
  lazy val identification: ProjectIdentification = ProjectIdentification(resourceId, path)

  def fold[P](rnp:  RenkuProject.WithoutParent => P,
              rwp:  RenkuProject.WithParent => P,
              nrnp: NonRenkuProject.WithoutParent => P,
              nrwp: NonRenkuProject.WithParent => P
  ): P
}

sealed trait NonRenkuProject extends Project with Product with Serializable {
  lazy val activities: List[Activity]                    = Nil
  lazy val datasets:   List[Dataset[Dataset.Provenance]] = Nil
  lazy val plans:      List[Plan]                        = Nil
}

sealed trait Parent {
  self: Project =>
  val parentResourceId: ResourceId
}

object NonRenkuProject {

  final case class WithoutParent(resourceId:       ResourceId,
                                 path:             Path,
                                 name:             Name,
                                 maybeDescription: Option[Description],
                                 dateCreated:      DateCreated,
                                 dateModified:     DateModified,
                                 maybeCreator:     Option[Person],
                                 visibility:       Visibility,
                                 keywords:         Set[Keyword],
                                 members:          Set[Person],
                                 images:           List[Image]
  ) extends NonRenkuProject {
    override def fold[P](rnp:  RenkuProject.WithoutParent => P,
                         rwp:  RenkuProject.WithParent => P,
                         nrnp: entities.NonRenkuProject.WithoutParent => P,
                         nrwp: NonRenkuProject.WithParent => P
    ): P = nrnp(this)
  }

  object WithoutParent extends ProjectFactory {
    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             dateCreated:      DateCreated,
             dateModified:     DateModified,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             images:           List[Image]
    ): ValidatedNel[String, NonRenkuProject.WithoutParent] =
      validateDates(dateCreated, dateModified).as(
        NonRenkuProject.WithoutParent(resourceId,
                                      path,
                                      name,
                                      maybeDescription,
                                      dateCreated,
                                      dateModified,
                                      maybeCreator,
                                      visibility,
                                      keywords,
                                      members,
                                      images
        )
      )
  }

  final case class WithParent(resourceId:       ResourceId,
                              path:             Path,
                              name:             Name,
                              maybeDescription: Option[Description],
                              dateCreated:      DateCreated,
                              dateModified:     DateModified,
                              maybeCreator:     Option[Person],
                              visibility:       Visibility,
                              keywords:         Set[Keyword],
                              members:          Set[Person],
                              parentResourceId: ResourceId,
                              images:           List[Image]
  ) extends NonRenkuProject
      with Parent {
    override def fold[P](rnp:  RenkuProject.WithoutParent => P,
                         rwp:  RenkuProject.WithParent => P,
                         nrnp: entities.NonRenkuProject.WithoutParent => P,
                         nrwp: NonRenkuProject.WithParent => P
    ): P = nrwp(this)
  }

  object WithParent extends ProjectFactory {
    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             dateCreated:      DateCreated,
             dateModified:     DateModified,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             parentResourceId: ResourceId,
             images:           List[Image]
    ): ValidatedNel[String, NonRenkuProject.WithParent] =
      validateDates(dateCreated, dateModified).as(
        NonRenkuProject.WithParent(resourceId,
                                   path,
                                   name,
                                   maybeDescription,
                                   dateCreated,
                                   dateModified,
                                   maybeCreator,
                                   visibility,
                                   keywords,
                                   members,
                                   parentResourceId,
                                   images
        )
      )
  }
}

sealed trait RenkuProject extends Project with Product with Serializable {
  val agent:      CliVersion
  val version:    SchemaVersion
  val activities: List[Activity]
  val datasets:   List[Dataset[Dataset.Provenance]]
  val plans:      List[Plan]
  val images:     List[Image]
}

object RenkuProject {
  final case class WithoutParent(resourceId:       ResourceId,
                                 path:             Path,
                                 name:             Name,
                                 maybeDescription: Option[Description],
                                 agent:            CliVersion,
                                 dateCreated:      DateCreated,
                                 dateModified:     DateModified,
                                 maybeCreator:     Option[Person],
                                 visibility:       Visibility,
                                 keywords:         Set[Keyword],
                                 members:          Set[Person],
                                 version:          SchemaVersion,
                                 activities:       List[Activity],
                                 datasets:         List[Dataset[Dataset.Provenance]],
                                 plans:            List[Plan],
                                 images:           List[Image]
  ) extends RenkuProject {
    override def fold[P](rnp:  RenkuProject.WithoutParent => P,
                         rwp:  RenkuProject.WithParent => P,
                         nrnp: entities.NonRenkuProject.WithoutParent => P,
                         nrwp: NonRenkuProject.WithParent => P
    ): P = rnp(this)
  }

  object WithoutParent extends ProjectFactory {

    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             agent:            CliVersion,
             dateCreated:      DateCreated,
             dateModified:     DateModified,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             version:          SchemaVersion,
             activities:       List[Activity],
             datasets:         List[Dataset[Dataset.Provenance]],
             plans:            List[Plan],
             images:           List[Image]
    ): ValidatedNel[String, RenkuProject.WithoutParent] = (
      validateDates(dateCreated, dateModified, activities, datasets, plans),
      validatePlansDates(plans),
      validateDatasets(datasets),
      updatePlansOriginalId(plans)
    ).mapN { (_, _, _, updatedPlans) =>
      val (syncedActivities, syncedDatasets, syncedPlans) =
        syncPersons(projectPersons = members ++ maybeCreator, activities, datasets, updatedPlans)
      RenkuProject.WithoutParent(
        resourceId,
        path,
        name,
        maybeDescription,
        agent,
        dateCreated,
        dateModified,
        maybeCreator,
        visibility,
        keywords,
        members,
        version,
        syncedActivities,
        syncedDatasets,
        syncedPlans,
        images
      )
    }

    private def validateDates(dateCreated:  DateCreated,
                              dateModified: DateModified,
                              activities:   List[Activity],
                              datasets:     List[Dataset[Provenance]],
                              plans:        List[Plan]
    ): ValidatedNel[String, Unit] =
      validateDates(dateCreated, dateModified) |+|
        validateDates(dateCreated, activitiesDates(activities)) |+|
        validateDates(dateCreated, datasetsDates(datasets)) |+|
        validateDates(dateCreated, planDates(plans))

    private type DatedEntity[R] = (String, R, InstantTinyType)

    private def activitiesDates(entities: List[Activity]): List[DatedEntity[activities.ResourceId]] =
      entities.map(a => ("Activity", a.resourceId, a.startTime))

    private def datasetsDates(entities: List[Dataset[Provenance]]): List[DatedEntity[datasets.ResourceId]] =
      entities.collect { ds =>
        ds.provenance match {
          case p: Provenance.Internal => ("Dataset", ds.resourceId, p.date)
          case p: Provenance.Modified => ("Dataset", ds.resourceId, p.date)
        }
      }

    private def planDates(entities: List[Plan]): List[DatedEntity[plans.ResourceId]] =
      entities.map(p => ("Plan", p.resourceId, p.dateCreated))

    private def validateDates[R](
        projectDate: DateCreated,
        toValidate:  List[DatedEntity[R]]
    )(implicit idShow: Show[R]) = {
      implicit lazy val show: Show[InstantTinyType] = Show.show(_.toString)

      toValidate
        .map { case (name, id, date) =>
          if (date.value >= projectDate.value) ().validNel[String]
          else show"$name $id date $date is older than project $projectDate".invalidNel
        }
        .sequence
        .void
    }
  }

  final case class WithParent(resourceId:       ResourceId,
                              path:             Path,
                              name:             Name,
                              maybeDescription: Option[Description],
                              agent:            CliVersion,
                              dateCreated:      DateCreated,
                              dateModified:     DateModified,
                              maybeCreator:     Option[Person],
                              visibility:       Visibility,
                              keywords:         Set[Keyword],
                              members:          Set[Person],
                              version:          SchemaVersion,
                              activities:       List[Activity],
                              datasets:         List[Dataset[Dataset.Provenance]],
                              plans:            List[Plan],
                              parentResourceId: ResourceId,
                              images:           List[Image]
  ) extends RenkuProject
      with Parent {
    override def fold[P](rnp:  RenkuProject.WithoutParent => P,
                         rwp:  RenkuProject.WithParent => P,
                         nrnp: entities.NonRenkuProject.WithoutParent => P,
                         nrwp: NonRenkuProject.WithParent => P
    ): P = rwp(this)
  }

  object WithParent extends ProjectFactory {

    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             agent:            CliVersion,
             dateCreated:      DateCreated,
             dateModified:     DateModified,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             version:          SchemaVersion,
             activities:       List[Activity],
             datasets:         List[Dataset[Dataset.Provenance]],
             plans:            List[Plan],
             parentResourceId: ResourceId,
             images:           List[Image]
    ): ValidatedNel[String, RenkuProject.WithParent] = (
      validateDates(dateCreated, dateModified),
      validateDatasets(datasets),
      validatePlansDates(plans),
      updatePlansOriginalId(plans),
      validateCompositePlanData(plans)
    ) mapN { (_, _, _, updatedPlans, _) =>
      val (syncedActivities, syncedDatasets, syncedPlans) =
        syncPersons(projectPersons = members ++ maybeCreator, activities, datasets, updatedPlans)
      RenkuProject.WithParent(
        resourceId,
        path,
        name,
        maybeDescription,
        agent,
        dateCreated,
        dateModified,
        maybeCreator,
        visibility,
        keywords,
        members,
        version,
        syncedActivities,
        syncedDatasets,
        syncedPlans,
        parentResourceId,
        images
      )
    }
  }

  trait ProjectFactory {

    protected def validateDates(dateCreated: DateCreated, dateModified: DateModified): ValidatedNel[String, Unit] =
      Validated.condNel(
        dateCreated.value <= dateModified.value,
        (),
        show"Project dateModified $dateModified is older than dateCreated $dateCreated"
      )

    protected def validateDatasets(datasets: List[Dataset[Provenance]]): ValidatedNel[String, Unit] = {
      val toDatasetsWithBrokenDerivedFrom: Dataset[Provenance] => Option[Dataset[Provenance.Modified]] = dataset =>
        dataset.provenance match {
          case prov: Dataset.Provenance.Modified =>
            datasets.find(_.resourceId.value == prov.derivedFrom.value) match {
              case Some(_) => Option.empty[Dataset[Provenance.Modified]]
              case _       => dataset.asInstanceOf[Dataset[Provenance.Modified]].some
            }
          case _ => Option.empty[Dataset[Provenance.Modified]]
        }

      datasets flatMap toDatasetsWithBrokenDerivedFrom match {
        case Nil => ().validNel[String]
        case first :: other =>
          NonEmptyList
            .of(first, other: _*)
            .map(ds =>
              show"Dataset ${ds.identification.identifier} is derived from non-existing dataset ${ds.provenance.derivedFrom}"
            )
            .invalid[Unit]
      }
    }

    protected def updatePlansOriginalId(planList: List[Plan]): ValidatedNel[String, List[Plan]] = {
      def findTopParent(derivedFrom: plans.DerivedFrom): ValidatedNel[String, Plan] =
        findParentPlan(derivedFrom, planList).andThen { parentPlan =>
          getPlanDerivation
            .get(parentPlan)
            .map(derivation => findTopParent(derivation.derivedFrom))
            .getOrElse(parentPlan.validNel)
        }

      planList.traverse { plan =>
        getPlanDerivation
          .get(plan)
          .map(derivation =>
            findTopParent(derivation.derivedFrom).map { topParent =>
              setPlanDerivation.modify(_.copy(originalResourceId = topParent.resourceId))(plan)
            }
          )
          .getOrElse(plan.validNel)
      }
    }

    private def findParentPlan(derivedFrom: plans.DerivedFrom, planList: List[Plan]) =
      Validated.fromOption(planList.find(_.resourceId.value == derivedFrom.value),
                           NonEmptyList.one(show"Cannot find parent plan $derivedFrom")
      )

    protected def validatePlansDates(planList: List[Plan]): ValidatedNel[String, Unit] =
      planList.traverse { plan =>
        getPlanDerivation
          .get(plan)
          .map { derivation =>
            findParentPlan(derivation.derivedFrom, planList)
              .andThen(parentPlan =>
                Validated.condNel[String, Plan](
                  plan.dateCreated.value >= parentPlan.dateCreated.value,
                  plan,
                  show"Plan ${plan.resourceId} is older than it's parent ${parentPlan.resourceId}"
                )
              )
          }
          .getOrElse(plan.validNel)
      }.void

    protected def validateCompositePlanData(projectPlans: List[Plan]): ValidatedNel[String, Unit] =
      projectPlans.collect { case p: CompositePlan => p } match {
        case Nil => Validated.validNel(())
        case cps =>
          val allPlans = projectPlans.groupMapReduce(_.resourceId)(identity)((a, _) => a)
          cps.traverse_ { cp =>
            val checkSubprocess = cp.plans.traverse_(validateSubprocessPlan(allPlans.keySet))
            val subSteps        = ProjectLens.collectAllSubPlans(allPlans)(cp)
            val subComp         = ProjectLens.collectAllSubCompositePlans(allPlans)(cp)
            val inputParamIds = subSteps.flatMap { p =>
              p.inputs.map(_.resourceId).toSet ++ p.parameters.map(_.resourceId).toSet
            }
            val outputIds = subSteps.flatMap { p =>
              p.outputs.map(_.resourceId).toSet
            }
            val mappingIds = subComp.flatMap { p =>
              p.mappings.map(_.resourceId).toSet
            }

            val checkMappings =
              cp.mappings.traverse_(validateParameterMapping(mappingIds ++ inputParamIds ++ outputIds))
            val checkLinks = cp.links.traverse_(validateLinks(outputIds, inputParamIds))

            checkLinks |+| checkMappings |+| checkSubprocess
          }
      }

    private def validateSubprocessPlan(projectPlans: Set[plans.ResourceId])(
        id: plans.ResourceId
    ): ValidatedNel[String, Unit] =
      Validated.condNel(projectPlans.contains(id), (), show"The subprocess plan $id is missing in the project.")

    private def validateParameterMapping(
        relevantIds: Set[commandParameters.ResourceId]
    )(pm: ParameterMapping): ValidatedNel[String, Unit] =
      pm.mappedParameter.traverse_ { id =>
        Validated.condNel(
          relevantIds.contains(id),
          (),
          show"ParameterMapping '$id' does not exist in the set of plans."
        )
      }

    private def validateLinks(
        outputIds:     Set[commandParameters.ResourceId],
        inputParamIds: Set[commandParameters.ResourceId]
    )(
        pl: ParameterLink
    ): ValidatedNel[String, Unit] = {
      val checkSource = Validated.condNel(
        outputIds.contains(pl.source),
        (),
        show"The source ${pl.source} is not available in the set of plans."
      )
      val checkSink = pl.sinks.traverse_(id =>
        Validated.condNel(inputParamIds.contains(id), (), show"The sink $id is not available in the set of plans")
      )

      (checkSource |+| checkSink).void
    }

    protected def syncPersons(projectPersons: Set[Person],
                              activities:     List[Activity],
                              datasets:       List[Dataset[Provenance]],
                              plans:          List[Plan]
    ): (List[Activity], List[Dataset[Provenance]], List[Plan]) = (
      activities.updatePersons(from = projectPersons),
      datasets.updateCreators(from = projectPersons),
      plans.updateCreators(from = projectPersons)
    )

    private implicit class ActivitiesOps(activities: List[Activity]) {

      def updatePersons(from: Set[Person]): List[Activity] =
        (updateAuthors(from) andThen updateAssociationAgents(from))(activities)

      private def updateAuthors(from: Set[Person]): List[Activity] => List[Activity] =
        activitiesLens.composeLens(ActivityLens.activityAuthor).modify { author =>
          from
            .find(byEmail(author))
            .getOrElse(author)
        }

      private def updateAssociationAgents(from: Set[Person]): List[Activity] => List[Activity] =
        activitiesLens.composeLens(ActivityLens.activityAssociationAgent).modify {
          case Right(person) =>
            Right(from.find(byEmail(person)).getOrElse(person))
          case other => other
        }
    }

    private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

      def updateCreators(from: Set[Person]): List[Dataset[Provenance]] =
        datasetsLens
          .composeLens(provenanceLens)
          .composeLens(provCreatorsLens)
          .composeTraversal(creatorsLens)
          .modify(creator => from.find(byEmail(creator)).getOrElse(creator))(datasets)
    }

    private implicit class PlansOps(plans: List[Plan]) {

      def updateCreators(from: Set[Person]): List[Plan] =
        plansLens
          .composeLens(PlanLens.planCreators)
          .modify(_.map(p => from.find(byEmail(p)).getOrElse(p)))(plans)
    }

    private lazy val byEmail: Person => Person => Boolean = { person1 => person2 =>
      (person1.maybeEmail -> person2.maybeEmail).mapN(_ == _).getOrElse(false)
    }

    private val activitiesLens: Traversal[List[Activity], Activity] = Traversal.fromTraverse[List, Activity]

    private val datasetsLens   = Traversal.fromTraverse[List, Dataset[Provenance]]
    private val provenanceLens = Lens[Dataset[Provenance], Provenance](_.provenance)(p => d => d.copy(provenance = p))
    private val creatorsLens   = Traversal.fromTraverse[NonEmptyList, Person]
    private val provCreatorsLens = Lens[Provenance, NonEmptyList[Person]](_.creators) { crts =>
      {
        case p: Provenance.Internal                         => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedExternal                 => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = crts.sortBy(_.name))
        case p: Provenance.Modified                         => p.copy(creators = crts.sortBy(_.name))
      }
    }

    private val plansLens = Traversal.fromTraverse[List, Plan]
  }
}

object Project {

  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  implicit def functions[P <: Project](implicit renkuUrl: RenkuUrl, glApiUrl: GitLabApiUrl): EntityFunctions[P] =
    new EntityFunctions[P] {

      override val findAllPersons: P => Set[Person] = { project =>
        project.members ++
          project.maybeCreator ++
          project.activities.flatMap(EntityFunctions[Activity].findAllPersons) ++
          project.datasets.flatMap(EntityFunctions[Dataset[Dataset.Provenance]].findAllPersons) ++
          project.plans.flatMap(EntityFunctions[Plan].findAllPersons)
      }

      override val encoder: GraphClass => JsonLDEncoder[P] = Project.encoder(renkuUrl, glApiUrl, _)
    }

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._

  val entityTypes: EntityTypes = EntityTypes.of(prov / "Location", schema / "Project")

  implicit def encoder[P <: Project](implicit
      renkuUrl: RenkuUrl,
      glApiUrl: GitLabApiUrl,
      graph:    GraphClass
  ): JsonLDEncoder[P] = JsonLDEncoder.instance {
    case project: RenkuProject =>
      val maybeDerivedFrom = project match {
        case p: RenkuProject.WithParent => p.parentResourceId.asEntityId.some
        case _ => None
      }
      JsonLD.arr(
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          renku / "projectPath"       -> project.path.asJsonLD,
          renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
          renku / "projectNamespaces" -> project.namespaces.asJsonLD,
          schema / "description"      -> project.maybeDescription.asJsonLD,
          schema / "agent"            -> project.agent.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "dateModified"     -> project.dateModified.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "keywords"         -> project.keywords.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD,
          renku / "hasActivity"       -> project.activities.asJsonLD,
          renku / "hasPlan"           -> project.plans.asJsonLD,
          renku / "hasDataset"        -> project.datasets.asJsonLD,
          prov / "wasDerivedFrom"     -> maybeDerivedFrom.asJsonLD,
          schema / "image"            -> project.images.asJsonLD
        ) :: project.datasets.flatMap(_.publicationEvents.map(_.asJsonLD)): _*
      )

    case project: NonRenkuProject =>
      val maybeDerivedFrom = project match {
        case p: NonRenkuProject.WithParent => p.parentResourceId.asEntityId.some
        case _ => None
      }
      JsonLD.arr {
        JsonLD.entity(
          project.resourceId.asEntityId,
          entityTypes,
          schema / "name"             -> project.name.asJsonLD,
          renku / "projectPath"       -> project.path.asJsonLD,
          renku / "projectNamespace"  -> project.path.toNamespace.asJsonLD,
          renku / "projectNamespaces" -> project.namespaces.asJsonLD,
          schema / "description"      -> project.maybeDescription.asJsonLD,
          schema / "dateCreated"      -> project.dateCreated.asJsonLD,
          schema / "dateModified"     -> project.dateModified.asJsonLD,
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "keywords"         -> project.keywords.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          prov / "wasDerivedFrom"     -> maybeDerivedFrom.asJsonLD,
          schema / "image"            -> project.images.asJsonLD
        )
      }
  }

  def fromCli(cliProject: CliProject, allPersons: Set[CliPerson], gitLabInfo: GitLabProjectInfo)(implicit
      renkuUrl: RenkuUrl
  ): ValidatedNel[String, Project] =
    CliProjectConverter.fromCli(cliProject, allPersons, gitLabInfo)

  object Ontology {

    val projectClass: Class = Class(schema / "Project")

    val creator: Property = schema / "creator"
    val image:   Property = schema / "image"

    val nameProperty:         DataProperty.Def = DataProperty(schema / "name", xsd / "string")
    val pathProperty:         DataProperty.Def = DataProperty(renku / "projectPath", xsd / "string")
    val descriptionProperty:  DataProperty.Def = DataProperty(schema / "description", xsd / "string")
    val dateCreatedProperty:  DataProperty.Def = DataProperty(schema / "dateCreated", xsd / "dateTime")
    val dateModifiedProperty: DataProperty.Def = DataProperty(schema / "dateModified", xsd / "dateTime")
    val visibilityProperty: DataProperty.Def = DataProperty.top(
      renku / "projectVisibility",
      DataPropertyRange(NonEmptyList.fromListUnsafe(projects.Visibility.all.toList))
    )
    val keywordsProperty: DataProperty.Def = DataProperty(schema / "keywords", xsd / "string")

    lazy val typeDef: Type =
      Type.Def(
        projectClass,
        ObjectProperties(
          ObjectProperty(schema / "agent", Agent.ontology),
          ObjectProperty(creator, Person.Ontology.typeDef),
          ObjectProperty(schema / "member", Person.Ontology.typeDef),
          ObjectProperty(renku / "hasActivity", Activity.ontology),
          ObjectProperty(renku / "hasPlan", Plan.ontology),
          ObjectProperty(renku / "hasDataset", Dataset.Ontology.typeDef),
          ObjectProperty(prov / "wasDerivedFrom", projectClass),
          ObjectProperty(image, Image.Ontology.typeDef)
        ),
        DataProperties(
          nameProperty,
          pathProperty,
          DataProperty(renku / "projectNamespace", xsd / "string"),
          DataProperty(renku / "projectNamespaces", xsd / "string"),
          descriptionProperty,
          dateCreatedProperty,
          dateModifiedProperty,
          visibilityProperty,
          keywordsProperty,
          DataProperty(schema / "schemaVersion", xsd / "string")
        )
      )
  }

  final case class GitLabProjectInfo(id:               GitLabId,
                                     name:             Name,
                                     path:             Path,
                                     dateCreated:      DateCreated,
                                     dateModified:     DateModified,
                                     maybeDescription: Option[Description],
                                     maybeCreator:     Option[ProjectMember],
                                     keywords:         Set[Keyword],
                                     members:          Set[ProjectMember],
                                     visibility:       Visibility,
                                     maybeParentPath:  Option[Path],
                                     avatarUrl:        Option[ImageUri]
  )

  sealed trait ProjectMember {
    val name:     persons.Name
    val username: persons.Username
    val gitLabId: persons.GitLabId
  }
  object ProjectMember {

    def apply(name: persons.Name, username: persons.Username, gitLabId: persons.GitLabId): ProjectMemberNoEmail =
      ProjectMemberNoEmail(name, username, gitLabId)

    final case class ProjectMemberNoEmail(name: persons.Name, username: persons.Username, gitLabId: persons.GitLabId)
        extends ProjectMember {

      def add(email: persons.Email): ProjectMemberWithEmail = ProjectMemberWithEmail(name, username, gitLabId, email)
    }

    final case class ProjectMemberWithEmail(name:     persons.Name,
                                            username: persons.Username,
                                            gitLabId: persons.GitLabId,
                                            email:    persons.Email
    ) extends ProjectMember
  }
}
