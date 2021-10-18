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
  val resourceId:   ResourceId
  val path:         Path
  val name:         Name
  val agent:        CliVersion
  val dateCreated:  DateCreated
  val maybeCreator: Option[Person]
  val visibility:   Visibility
  val members:      Set[Person]
  val version:      SchemaVersion
  val activities:   List[Activity]
  val datasets:     List[Dataset[Dataset.Provenance]]

  lazy val plans:      Set[Plan]       = activities.map(_.association.plan).toSet
  lazy val namespaces: List[Namespace] = path.toNamespaces
}

final case class ProjectWithoutParent(resourceId:   ResourceId,
                                      path:         Path,
                                      name:         Name,
                                      agent:        CliVersion,
                                      dateCreated:  DateCreated,
                                      maybeCreator: Option[Person],
                                      visibility:   Visibility,
                                      members:      Set[Person],
                                      version:      SchemaVersion,
                                      activities:   List[Activity],
                                      datasets:     List[Dataset[Dataset.Provenance]]
) extends Project

object ProjectWithoutParent extends ProjectFactory {

  def from(resourceId:   ResourceId,
           path:         Path,
           name:         Name,
           agent:        CliVersion,
           dateCreated:  DateCreated,
           maybeCreator: Option[Person],
           visibility:   Visibility,
           members:      Set[Person],
           version:      SchemaVersion,
           activities:   List[Activity],
           datasets:     List[Dataset[Dataset.Provenance]]
  ): ValidatedNel[String, ProjectWithoutParent] =
    (validateDates(dateCreated, activities, datasets), validateDatasets(datasets))
      .mapN { (_, _) =>
        val (syncedActivities, syncedDatasets) =
          syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
        ProjectWithoutParent(resourceId,
                             path,
                             name,
                             agent,
                             dateCreated,
                             maybeCreator,
                             visibility,
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
      .void
}

final case class ProjectWithParent(resourceId:       ResourceId,
                                   path:             Path,
                                   name:             Name,
                                   agent:            CliVersion,
                                   dateCreated:      DateCreated,
                                   maybeCreator:     Option[Person],
                                   visibility:       Visibility,
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
           agent:            CliVersion,
           dateCreated:      DateCreated,
           maybeCreator:     Option[Person],
           visibility:       Visibility,
           members:          Set[Person],
           version:          SchemaVersion,
           activities:       List[Activity],
           datasets:         List[Dataset[Dataset.Provenance]],
           parentResourceId: ResourceId
  ): ValidatedNel[String, ProjectWithParent] = validateDatasets(datasets) map { _ =>
    val (syncedActivities, syncedDatasets) =
      syncPersons(projectPersons = members ++ maybeCreator, activities, datasets)
    ProjectWithParent(resourceId,
                      path,
                      name,
                      agent,
                      dateCreated,
                      maybeCreator,
                      visibility,
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
    activities.updateAuthors(from = projectPersons) -> datasets.updateCreators(from = projectPersons)

  private implicit class ActivitiesOps(activities: List[Activity]) {

    def updateAuthors(from: Set[Person]): List[Activity] =
      activitiesLens.modify { activity =>
        from
          .find(_.resourceId == activity.author.resourceId)
          .map(person => activityAuthorLens.modify(_ => person)(activity))
          .getOrElse(activity)
      }(activities)
  }

  private implicit class DatasetsOps(datasets: List[Dataset[Provenance]]) {

    def updateCreators(from: Set[Person]): List[Dataset[Provenance]] = {
      val creatorsUpdate = creatorsLens.modify { person =>
        from.find(_.resourceId == person.resourceId).getOrElse(person)
      }
      datasetsLens.modify(
        provenanceLens.modify(provCreatorsLens.modify(creatorsUpdate))
      )(datasets)
    }
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
        schema / "agent"            -> project.agent.asJsonLD,
        schema / "dateCreated"      -> project.dateCreated.asJsonLD,
        schema / "creator"          -> project.maybeCreator.asJsonLD,
        renku / "projectVisibility" -> project.visibility.asJsonLD,
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

  final case class GitLabProjectInfo(name:            Name,
                                     path:            Path,
                                     dateCreated:     DateCreated,
                                     maybeCreator:    Option[ProjectMember],
                                     members:         Set[ProjectMember],
                                     visibility:      Visibility,
                                     maybeParentPath: Option[Path]
  )

  final case class ProjectMember(name: users.Name, username: users.Username, gitLabId: users.GitLabId)
}
