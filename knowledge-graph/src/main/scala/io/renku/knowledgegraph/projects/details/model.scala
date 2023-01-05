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

package io.renku.knowledgegraph.projects.details

import cats.data.Validated
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import io.circe.Decoder
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.{DateCreated, Description, GitLabId, Keyword, Name, Path, ResourceId, Visibility}
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.graph.model.{SchemaVersion, persons}
import io.renku.tinytypes._
import io.renku.tinytypes.constraints._
import model.Statistics._

import java.net.{MalformedURLException, URL}
import java.time.Instant

private object model {
  import Forking.ForksCount
  import Project._
  import Urls._

  final case class Project(resourceId:       ResourceId,
                           id:               GitLabId,
                           path:             Path,
                           name:             Name,
                           maybeDescription: Option[Description],
                           visibility:       Visibility,
                           created:          Creation,
                           updatedAt:        DateUpdated,
                           urls:             Urls,
                           forking:          Forking,
                           keywords:         Set[Keyword],
                           starsCount:       StarsCount,
                           permissions:      Permissions,
                           statistics:       Statistics,
                           maybeVersion:     Option[SchemaVersion],
                           images:           List[ImageUri]
  )

  object Project {
    final case class ImageLinks(location: ImageUri)
    object ImageLinks {
      implicit val jsonDecoder: Decoder[ImageLinks] =
        io.circe.generic.semiauto.deriveDecoder[ImageLinks]
    }

    final class StarsCount private (val value: Int) extends AnyVal with IntTinyType
    implicit object StarsCount extends TinyTypeFactory[StarsCount](new StarsCount(_)) with NonNegativeInt[StarsCount]

    final class DateUpdated private (val value: Instant) extends AnyVal with InstantTinyType
    implicit object DateUpdated
        extends TinyTypeFactory[DateUpdated](new DateUpdated(_))
        with InstantNotInTheFuture[DateUpdated]
        with TinyTypeJsonLDOps[DateUpdated]
  }

  final case class Creation(date: DateCreated, maybeCreator: Option[Creator])

  final case class Creator(resourceId:       persons.ResourceId,
                           name:             persons.Name,
                           maybeEmail:       Option[persons.Email],
                           maybeAffiliation: Option[persons.Affiliation]
  )

  final case class Forking(forksCount: ForksCount, maybeParent: Option[ParentProject])

  object Forking {
    final class ForksCount private (val value: Int) extends AnyVal with IntTinyType
    implicit object ForksCount extends TinyTypeFactory[ForksCount](new ForksCount(_)) with NonNegativeInt[ForksCount]
  }

  final case class ParentProject(resourceId: ResourceId, path: Path, name: Name, created: Creation)

  sealed trait Permissions extends Product with Serializable

  object Permissions {

    final case class ProjectPermissions(projectAccessLevel: ProjectAccessLevel) extends Permissions
    final case class GroupPermissions(groupAccessLevel: GroupAccessLevel)       extends Permissions
    final case class ProjectAndGroupPermissions(projectAccessLevel: ProjectAccessLevel,
                                                groupAccessLevel:   GroupAccessLevel
    ) extends Permissions

    def apply(accessLevel: ProjectAccessLevel): Permissions = ProjectPermissions(accessLevel)
    def apply(accessLevel: GroupAccessLevel):   Permissions = GroupPermissions(accessLevel)
    def apply(projectAccessLevel: ProjectAccessLevel, groupAccessLevel: GroupAccessLevel): Permissions =
      ProjectAndGroupPermissions(projectAccessLevel, groupAccessLevel)

    final case class ProjectAccessLevel(accessLevel: AccessLevel) extends AccessLevel {
      override val name:  Refined[String, NonEmpty] = accessLevel.name
      override val value: Refined[Int, Positive]    = accessLevel.value
    }
    final case class GroupAccessLevel(accessLevel: AccessLevel) extends AccessLevel {
      override val name:  Refined[String, NonEmpty] = accessLevel.name
      override val value: Refined[Int, Positive]    = accessLevel.value
    }

    sealed trait AccessLevel extends Product with Serializable {
      val name:  String Refined NonEmpty
      val value: Int Refined Positive
      override lazy val toString: String = s"$name ($value)"
    }

    object AccessLevel {

      def from(value: Int): Either[IllegalArgumentException, AccessLevel] = Either.fromOption(
        all.find(_.value.value == value),
        ifNone = new IllegalArgumentException(s"Unrecognized AccessLevel with value '$value'")
      )

      sealed abstract class AbstractAccessLevel(val name: String Refined NonEmpty, val value: Int Refined Positive)
          extends AccessLevel

      final case object Guest      extends AbstractAccessLevel(name = "Guest", value = 10)
      final case object Reporter   extends AbstractAccessLevel(name = "Reporter", value = 20)
      final case object Developer  extends AbstractAccessLevel(name = "Developer", value = 30)
      final case object Maintainer extends AbstractAccessLevel(name = "Maintainer", value = 40)
      final case object Owner      extends AbstractAccessLevel(name = "Owner", value = 50)

      lazy val all: Set[AccessLevel] = Set(Guest, Reporter, Developer, Maintainer, Owner)
    }
  }

  final case class Statistics(commitsCount:     CommitsCount,
                              storageSize:      StorageSize,
                              repositorySize:   RepositorySize,
                              lsfObjectsSize:   LsfObjectsSize,
                              jobArtifactsSize: JobArtifactsSize
  )

  object Statistics {
    final class CommitsCount private (val value: Long) extends AnyVal with LongTinyType
    implicit object CommitsCount
        extends TinyTypeFactory[CommitsCount](new CommitsCount(_))
        with NonNegativeLong[CommitsCount]

    final class StorageSize private (val value: Long) extends AnyVal with LongTinyType
    implicit object StorageSize
        extends TinyTypeFactory[StorageSize](new StorageSize(_))
        with NonNegativeLong[StorageSize]

    final class RepositorySize private (val value: Long) extends AnyVal with LongTinyType
    implicit object RepositorySize
        extends TinyTypeFactory[RepositorySize](new RepositorySize(_))
        with NonNegativeLong[RepositorySize]

    final class LsfObjectsSize private (val value: Long) extends AnyVal with LongTinyType
    implicit object LsfObjectsSize
        extends TinyTypeFactory[LsfObjectsSize](new LsfObjectsSize(_))
        with NonNegativeLong[LsfObjectsSize]

    final class JobArtifactsSize private (val value: Long) extends AnyVal with LongTinyType
    implicit object JobArtifactsSize
        extends TinyTypeFactory[JobArtifactsSize](new JobArtifactsSize(_))
        with NonNegativeLong[JobArtifactsSize]
  }

  final case class Urls(ssh: SshUrl, http: HttpUrl, web: WebUrl, maybeReadme: Option[ReadmeUrl])

  object Urls {

    final class SshUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object SshUrl extends TinyTypeFactory[SshUrl](new SshUrl(_)) with NonBlank[SshUrl] {
      addConstraint(
        check = _ matches "^git@.*\\.git$",
        message = url => s"$url is not a valid repository ssh url"
      )
    }

    final class HttpUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object HttpUrl extends TinyTypeFactory[HttpUrl](new HttpUrl(_)) with NonBlank[HttpUrl] {
      addConstraint(
        check = url =>
          (url endsWith ".git") && Validated
            .catchOnly[MalformedURLException](new URL(url))
            .isValid,
        message = url => s"$url is not a valid repository http url"
      )
    }

    final class WebUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object WebUrl                         extends TinyTypeFactory[WebUrl](new WebUrl(_)) with Url[WebUrl]

    final class ReadmeUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object ReadmeUrl extends TinyTypeFactory[ReadmeUrl](new ReadmeUrl(_)) with Url[ReadmeUrl]
  }
}
