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
import monocle.{Lens, Traversal}

sealed trait Project extends Product with Serializable {
  val resourceId:       ResourceId
  val path:             Path
  val name:             Name
  val maybeDescription: Option[Description]
  val agent:            CliVersion
  val dateCreated:      DateCreated
  val maybeCreator:     Option[Person]
  val visibility:       Visibility
  val keywords:         Set[Keyword]
  val members:          Set[Person]
  val version:          SchemaVersion
  val activities:       List[Activity]
  val datasets:         List[Dataset[Dataset.Provenance]]

  lazy val plans:      Set[Plan]       = activities.map(_.association.plan).toSet
  lazy val namespaces: List[Namespace] = path.toNamespaces
}

final case class ProjectWithoutParent(resourceId:       ResourceId,
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
) extends Project

object ProjectWithoutParent extends ProjectFactory {

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
  ): ValidatedNel[String, ProjectWithoutParent] =
    (validateDates(dateCreated, activities, datasets), validateDatasets(datasets))
      .mapN { (_, _) =>
        val (syncedActivities, syncedDatasets) =
          syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
        ProjectWithoutParent(resourceId,
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

final case class ProjectWithParent(resourceId:       ResourceId,
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
) extends Project

object ProjectWithParent extends ProjectFactory {

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
  ): ValidatedNel[String, ProjectWithParent] = validateDatasets(datasets) map { _ =>
    val (syncedActivities, syncedDatasets) =
      syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
    ProjectWithParent(
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
      (updateAuthors(from) andThen updateAssociationAgents(from))(activities)

    private def updateAuthors(from: Set[Person]): List[Activity] => List[Activity] =
      activitiesLens.modify { activity =>
        from
          .find(byEmail(activity.author))
          .map(person => activityAuthorLens.modify(_ => person)(activity))
          .getOrElse(activity)
      }

    private def updateAssociationAgents(from: Set[Person]): List[Activity] => List[Activity] =
      activitiesLens.modify { activity =>
        activity.association match {
          case _:     Association.WithRenkuAgent => activity
          case assoc: Association.WithPersonAgent =>
            from
              .find(byEmail(assoc.agent))
              .map(person => activity.copy(association = assoc.copy(agent = person)))
              .getOrElse(activity)
        }
      }
  }

  private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

    def updateCreators(from: Set[Person]): List[Dataset[Provenance]] = {
      val creatorsUpdate = creatorsLens.modify { creator =>
        from.find(byEmail(creator)).getOrElse(creator)
      }
      datasetsLens.modify(
        provenanceLens.modify(provCreatorsLens.modify(creatorsUpdate))
      )(datasets)
    }
  }

  private lazy val byEmail: Person => Person => Boolean = { person1 => person2 =>
    (person1.maybeEmail -> person2.maybeEmail).mapN(_ == _).getOrElse(false)
  }

  private val activitiesLens: Traversal[List[Activity], Activity] = Traversal.fromTraverse[List, Activity]
  private val activityAuthorLens = Lens[Activity, Person](_.author)(p => a => a.copy(author = p))

  private val datasetsLens   = Traversal.fromTraverse[List, Dataset[Provenance]]
  private val provenanceLens = Lens[Dataset[Provenance], Provenance](_.provenance)(p => d => d.copy(provenance = p))
  private val creatorsLens   = Traversal.fromTraverse[List, Person]
  private val provCreatorsLens = Lens[Provenance, List[Person]](_.creators.toList) { crts =>
    {
      case p: Provenance.Internal                         => p.copy(creators = crts.toSet)
      case p: Provenance.ImportedExternal                 => p.copy(creators = crts.toSet)
      case p: Provenance.ImportedInternalAncestorExternal => p.copy(creators = crts.toSet)
      case p: Provenance.ImportedInternalAncestorInternal => p.copy(creators = crts.toSet)
      case p: Provenance.Modified                         => p.copy(creators = crts.toSet)
    }
  }
}

object Project {

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}

  val entityTypes: EntityTypes = EntityTypes.of(prov / "Location", schema / "Project")

  implicit def encoder[P <: Project](implicit
      renkuBaseUrl: RenkuBaseUrl,
      gitLabApiUrl: GitLabApiUrl
  ): JsonLDEncoder[P] = JsonLDEncoder.instance { project =>
    val maybeDerivedFrom = project match {
      case p: ProjectWithParent => p.parentResourceId.asEntityId.some
      case _ => None
    }
    JsonLD.arr(
      JsonLD.entity(
        project.resourceId.asEntityId,
        entityTypes,
        schema / "name"             -> project.name.asJsonLD,
        renku / "projectPath"       -> project.path.asJsonLD,
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
  }

  def decoder(gitLabInfo: GitLabProjectInfo)(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDDecoder[Project] =
    ProjectJsonLDDecoder(gitLabInfo)

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
    val name:     users.Name
    val username: users.Username
    val gitLabId: users.GitLabId
  }
  object ProjectMember {

    def apply(name: users.Name, username: users.Username, gitLabId: users.GitLabId): ProjectMemberNoEmail =
      ProjectMemberNoEmail(name, username, gitLabId)

    final case class ProjectMemberNoEmail(name: users.Name, username: users.Username, gitLabId: users.GitLabId)
        extends ProjectMember {

      def add(email: users.Email): ProjectMemberWithEmail = ProjectMemberWithEmail(name, username, gitLabId, email)
    }

    final case class ProjectMemberWithEmail(name:     users.Name,
                                            username: users.Username,
                                            gitLabId: users.GitLabId,
                                            email:    users.Email
    ) extends ProjectMember
  }
}
