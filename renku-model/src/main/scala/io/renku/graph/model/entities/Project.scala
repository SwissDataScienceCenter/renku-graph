/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all._
import io.renku.graph.model
import io.renku.graph.model._
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.projects._
import io.renku.jsonld.JsonLDDecoder
import io.renku.jsonld.ontology._
import monocle.{Lens, Traversal}

sealed trait Project extends Product with Serializable {
  val resourceId:       ResourceId
  val path:             Path
  val name:             Name
  val maybeDescription: Option[Description]
  val dateCreated:      DateCreated
  val maybeCreator:     Option[Person]
  val visibility:       Visibility
  val keywords:         Set[Keyword]
  val members:          Set[Person]

  val activities: List[Activity]
  val datasets:   List[Dataset[Dataset.Provenance]]
  val plans:      Set[Plan]
  lazy val namespaces: List[Namespace] = path.toNamespaces
}

sealed trait NonRenkuProject extends Project with Product with Serializable {
  lazy val activities: List[Activity]                    = Nil
  lazy val datasets:   List[Dataset[Dataset.Provenance]] = Nil
  lazy val plans:      Set[Plan]                         = Set.empty
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
                                 maybeCreator:     Option[Person],
                                 visibility:       Visibility,
                                 keywords:         Set[Keyword],
                                 members:          Set[Person]
  ) extends NonRenkuProject

  final case class WithParent(resourceId:       ResourceId,
                              path:             Path,
                              name:             Name,
                              maybeDescription: Option[Description],
                              dateCreated:      DateCreated,
                              maybeCreator:     Option[Person],
                              visibility:       Visibility,
                              keywords:         Set[Keyword],
                              members:          Set[Person],
                              parentResourceId: ResourceId
  ) extends NonRenkuProject
      with Parent
}

sealed trait RenkuProject extends Project with Product with Serializable {
  val agent:      CliVersion
  val version:    SchemaVersion
  val activities: List[Activity]
  val datasets:   List[Dataset[Dataset.Provenance]]

  lazy val plans: Set[Plan] = activities.map(_.association.plan).toSet
}

object RenkuProject {
  final case class WithoutParent(resourceId:       ResourceId,
                                 path:             Path,
                                 name:             Name,
                                 maybeDescription: Option[Description],
                                 agent:            CliVersion,
                                 dateCreated:      DateCreated,
                                 maybeCreator:     Option[Person],
                                 visibility:       Visibility,
                                 keywords:         Set[Keyword],
                                 members:          Set[Person],
                                 version:          SchemaVersion,
                                 activities:       List[Activity],
                                 datasets:         List[Dataset[Dataset.Provenance]]
  ) extends RenkuProject

  object WithoutParent extends ProjectFactory {

    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             agent:            CliVersion,
             dateCreated:      DateCreated,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             version:          SchemaVersion,
             activities:       List[Activity],
             datasets:         List[Dataset[Dataset.Provenance]]
    ): ValidatedNel[String, RenkuProject.WithoutParent] =
      (validateDates(dateCreated, activities, datasets), validateDatasets(datasets))
        .mapN { (_, _) =>
          val (syncedActivities, syncedDatasets) =
            syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
          RenkuProject.WithoutParent(resourceId,
                                     path,
                                     name,
                                     maybeDescription,
                                     agent,
                                     dateCreated,
                                     maybeCreator,
                                     visibility,
                                     keywords,
                                     members,
                                     version,
                                     syncedActivities,
                                     syncedDatasets
          )
        }

    private def validateDates(dateCreated: DateCreated,
                              activities:  List[Activity],
                              datasets:    List[Dataset[Provenance]]
    ): ValidatedNel[String, Unit] =
      activities
        .map { activity =>
          import activity._
          if ((startTime.value compareTo dateCreated.value) >= 0) ().validNel[String]
          else s"Activity $resourceId startTime $startTime is older than project $dateCreated".invalidNel
        }
        .sequence
        .void |+| datasets
        .map {
          def compareDateWithProject(dataset: Dataset[Provenance], dsCreated: model.datasets.DateCreated) =
            if ((dsCreated.value compareTo dateCreated.value) >= 0) ().validNel[String]
            else
              s"Dataset ${dataset.identification.identifier} date $dsCreated is older than project $dateCreated".invalidNel

          dataset =>
            dataset.provenance match {
              case p: Provenance.Internal => compareDateWithProject(dataset, p.date)
              case p: Provenance.Modified => compareDateWithProject(dataset, p.date)
              case _ => ().validNel[String]
            }
        }
        .sequence
        .void |+| activities
        .map { activity =>
          import activity.association.plan
          if ((plan.dateCreated.value compareTo dateCreated.value) >= 0) ().validNel[String]
          else s"Plan ${plan.resourceId} dateCreated ${plan.dateCreated} is older than project $dateCreated".invalidNel
        }
        .sequence
        .void
  }

  final case class WithParent(resourceId:       ResourceId,
                              path:             Path,
                              name:             Name,
                              maybeDescription: Option[Description],
                              agent:            CliVersion,
                              dateCreated:      DateCreated,
                              maybeCreator:     Option[Person],
                              visibility:       Visibility,
                              keywords:         Set[Keyword],
                              members:          Set[Person],
                              version:          SchemaVersion,
                              activities:       List[Activity],
                              datasets:         List[Dataset[Dataset.Provenance]],
                              parentResourceId: ResourceId
  ) extends RenkuProject
      with Parent

  object WithParent extends ProjectFactory {

    def from(resourceId:       ResourceId,
             path:             Path,
             name:             Name,
             maybeDescription: Option[Description],
             agent:            CliVersion,
             dateCreated:      DateCreated,
             maybeCreator:     Option[Person],
             visibility:       Visibility,
             keywords:         Set[Keyword],
             members:          Set[Person],
             version:          SchemaVersion,
             activities:       List[Activity],
             datasets:         List[Dataset[Dataset.Provenance]],
             parentResourceId: ResourceId
    ): ValidatedNel[String, RenkuProject.WithParent] = validateDatasets(datasets) map { _ =>
      val (syncedActivities, syncedDatasets) =
        syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
      RenkuProject.WithParent(
        resourceId,
        path,
        name,
        maybeDescription,
        agent,
        dateCreated,
        maybeCreator,
        visibility,
        keywords,
        members,
        version,
        syncedActivities,
        syncedDatasets,
        parentResourceId
      )
    }
  }

  trait ProjectFactory {

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

    protected def syncPersons(projectPersons: Set[Person],
                              activities:     List[Activity],
                              datasets:       List[Dataset[Provenance]]
    ): (List[Activity], List[Dataset[Provenance]]) =
      activities.updatePersons(from = projectPersons) -> datasets.updateCreators(from = projectPersons)

    private implicit class ActivitiesOps(activities: List[Activity]) {

      def updatePersons(from: Set[Person]): List[Activity] =
        (updateAuthors(from) andThen updateAssociationAgents(from) andThen updatePlanCreators(from))(activities)

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

      private def updatePlanCreators(from: Set[Person]): List[Activity] => List[Activity] =
        activitiesLens
          .composeLens(ActivityLens.activityPlanCreators)
          .modify(ps => ps.map(p => from.find(byEmail(p)).getOrElse(p)))
    }

    private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

      def updateCreators(from: Set[Person]): List[Dataset[Provenance]] =
        datasetsLens
          .composeLens(provenanceLens)
          .composeLens(provCreatorsLens)
          .composeTraversal(creatorsLens)
          .modify(creator => from.find(byEmail(creator)).getOrElse(creator))(datasets)
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
          project.datasets.flatMap(EntityFunctions[Dataset[Dataset.Provenance]].findAllPersons)
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
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "keywords"         -> project.keywords.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          schema / "schemaVersion"    -> project.version.asJsonLD,
          renku / "hasActivity"       -> project.activities.asJsonLD,
          renku / "hasPlan"           -> project.plans.toList.asJsonLD,
          renku / "hasDataset"        -> project.datasets.asJsonLD,
          prov / "wasDerivedFrom"     -> maybeDerivedFrom.asJsonLD
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
          schema / "creator"          -> project.maybeCreator.asJsonLD,
          renku / "projectVisibility" -> project.visibility.asJsonLD,
          schema / "keywords"         -> project.keywords.asJsonLD,
          schema / "member"           -> project.members.toList.asJsonLD,
          prov / "wasDerivedFrom"     -> maybeDerivedFrom.asJsonLD
        )
      }
  }

  def decoder(gitLabInfo: GitLabProjectInfo)(implicit renkuUrl: RenkuUrl): JsonLDDecoder[Project] =
    ProjectJsonLDDecoder(gitLabInfo)

  lazy val ontology: Type = {
    val projectClass = Class(schema / "Project")
    Type.Def(
      projectClass,
      ObjectProperties(
        ObjectProperty(schema / "agent", Agent.ontology),
        ObjectProperty(schema / "creator", Person.ontology),
        ObjectProperty(schema / "member", Person.ontology),
        ObjectProperty(renku / "hasActivity", Activity.ontology),
        ObjectProperty(renku / "hasPlan", Plan.ontology),
        ObjectProperty(renku / "hasDataset", Dataset.ontology),
        ObjectProperty(prov / "wasDerivedFrom", projectClass)
      ),
      DataProperties(
        DataProperty(schema / "name", xsd / "string"),
        DataProperty(renku / "projectPath", xsd / "string"),
        DataProperty(renku / "projectNamespace", xsd / "string"),
        DataProperty(renku / "projectNamespaces", xsd / "string"),
        DataProperty(schema / "description", xsd / "string"),
        DataProperty(schema / "dateCreated", xsd / "dateTime"),
        DataProperty.top(renku / "projectVisibility",
                         DataPropertyRange(NonEmptyList.fromListUnsafe(projects.Visibility.all.toList))
        ),
        DataProperty(schema / "keywords", xsd / "string"),
        DataProperty(schema / "schemaVersion", xsd / "string")
      )
    )
  }

  final case class GitLabProjectInfo(id:               Id,
                                     name:             Name,
                                     path:             Path,
                                     dateCreated:      DateCreated,
                                     maybeDescription: Option[Description],
                                     maybeCreator:     Option[ProjectMember],
                                     keywords:         Set[Keyword],
                                     members:          Set[ProjectMember],
                                     visibility:       Visibility,
                                     maybeParentPath:  Option[Path]
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
