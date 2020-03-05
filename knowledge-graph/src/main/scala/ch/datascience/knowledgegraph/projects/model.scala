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

package ch.datascience.knowledgegraph.projects

import java.net.{MalformedURLException, URL}
import java.time.Instant

import cats.data.Validated
import cats.implicits._
import ch.datascience.graph.model.projects.{DateCreated, Description, Id, Name, Path, Visibility}
import ch.datascience.graph.model.users
import ch.datascience.knowledgegraph.projects.model.Statistics._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive

object model {
  import Forking.ForksCount
  import Permissions.AccessLevel
  import Project._
  import Urls._

  final case class Project(id:               Id,
                           path:             Path,
                           name:             Name,
                           maybeDescription: Option[Description],
                           visibility:       Visibility,
                           created:          Creation,
                           updatedAt:        DateUpdated,
                           urls:             Urls,
                           forking:          Forking,
                           tags:             Set[Tag],
                           starsCount:       StarsCount,
                           permissions:      Permissions,
                           statistics:       Statistics)

  object Project {
    final class Tag private (val value: String) extends AnyVal with StringTinyType
    implicit object Tag extends TinyTypeFactory[Tag](new Tag(_)) with NonBlank

    final class StarsCount private (val value: Int) extends AnyVal with IntTinyType
    implicit object StarsCount extends TinyTypeFactory[StarsCount](new StarsCount(_)) with NonNegativeInt

    final class DateUpdated private (val value: Instant) extends AnyVal with InstantTinyType
    implicit object DateUpdated extends TinyTypeFactory[DateUpdated](new DateUpdated(_)) with InstantNotInTheFuture
  }

  final case class Creation(date: DateCreated, creator: Creator)

  final case class Creator(email: users.Email, name: users.Name)

  final case class Forking(forksCount: ForksCount, maybeParent: Option[ParentProject])

  object Forking {
    final class ForksCount private (val value: Int) extends AnyVal with IntTinyType
    implicit object ForksCount extends TinyTypeFactory[ForksCount](new ForksCount(_)) with NonNegativeInt
  }

  final case class ParentProject(id: Id, path: Path, name: Name)

  final case class Permissions(projectAccessLevel: AccessLevel, maybeGroupAccessLevel: Option[AccessLevel])

  object Permissions {

    sealed abstract class AccessLevel(val name: String Refined NonEmpty, val value: Int Refined Positive)
        extends Product
        with Serializable {
      override lazy val toString: String = s"$name ($value)"
    }

    object AccessLevel {

      def from(value: Int): Either[IllegalArgumentException, AccessLevel] = Either.fromOption(
        all.find(_.value.value == value),
        ifNone = new IllegalArgumentException(s"Unrecognized AccessLevel with value '$value'")
      )

      lazy val all: Set[AccessLevel] = Set(Guest, Reporter, Developer, Maintainer, Owner)

      final case object Guest      extends AccessLevel(name = "Guest", value      = 10)
      final case object Reporter   extends AccessLevel(name = "Reporter", value   = 20)
      final case object Developer  extends AccessLevel(name = "Developer", value  = 30)
      final case object Maintainer extends AccessLevel(name = "Maintainer", value = 40)
      final case object Owner      extends AccessLevel(name = "Owner", value      = 50)
    }
  }

  final case class Statistics(commitsCount:     CommitsCount,
                              storageSize:      StorageSize,
                              repositorySize:   RepositorySize,
                              lsfObjectsSize:   LsfObjectsSize,
                              jobArtifactsSize: JobArtifactsSize)

  object Statistics {
    final class CommitsCount private (val value: Int) extends AnyVal with IntTinyType
    implicit object CommitsCount extends TinyTypeFactory[CommitsCount](new CommitsCount(_)) with NonNegativeInt

    final class StorageSize private (val value: Int) extends AnyVal with IntTinyType
    implicit object StorageSize extends TinyTypeFactory[StorageSize](new StorageSize(_)) with NonNegativeInt

    final class RepositorySize private (val value: Int) extends AnyVal with IntTinyType
    implicit object RepositorySize extends TinyTypeFactory[RepositorySize](new RepositorySize(_)) with NonNegativeInt

    final class LsfObjectsSize private (val value: Int) extends AnyVal with IntTinyType
    implicit object LsfObjectsSize extends TinyTypeFactory[LsfObjectsSize](new LsfObjectsSize(_)) with NonNegativeInt

    final class JobArtifactsSize private (val value: Int) extends AnyVal with IntTinyType
    implicit object JobArtifactsSize
        extends TinyTypeFactory[JobArtifactsSize](new JobArtifactsSize(_))
        with NonNegativeInt
  }

  final case class Urls(ssh: SshUrl, http: HttpUrl, web: WebUrl, readme: ReadmeUrl)

  object Urls {

    final class SshUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object SshUrl extends TinyTypeFactory[SshUrl](new SshUrl(_)) with NonBlank {
      addConstraint(
        check   = _ matches "^git@.*\\.git$",
        message = url => s"$url is not a valid repository ssh url"
      )
    }

    final class HttpUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object HttpUrl extends TinyTypeFactory[HttpUrl](new HttpUrl(_)) with NonBlank {
      addConstraint(
        check = url =>
          (url endsWith ".git") && Validated
            .catchOnly[MalformedURLException](new URL(url))
            .isValid,
        message = url => s"$url is not a valid repository http url"
      )
    }

    final class WebUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object WebUrl extends TinyTypeFactory[WebUrl](new WebUrl(_)) with Url

    final class ReadmeUrl private (val value: String) extends AnyVal with StringTinyType
    implicit object ReadmeUrl extends TinyTypeFactory[ReadmeUrl](new ReadmeUrl(_)) with Url
  }
}
