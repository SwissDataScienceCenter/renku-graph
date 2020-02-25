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

import cats.data.Validated
import ch.datascience.graph.model.projects.{DateCreated, Description, Id, Name, Path, Visibility}
import ch.datascience.graph.model.users
import ch.datascience.knowledgegraph.projects.model.RepoUrls._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{DateUpdated, ForksCount, StarsCount}
import ch.datascience.tinytypes.constraints.{NonBlank, Url}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

object model {

  final case class Project(id:               Id,
                           path:             Path,
                           name:             Name,
                           maybeDescription: Option[Description],
                           visibility:       Visibility,
                           created:          Creation,
                           repoUrls:         RepoUrls,
                           forks:            Forks,
                           starsCount:       StarsCount,
                           updatedAt:        DateUpdated)

  final case class Creation(date: DateCreated, creator: Creator)

  final case class Creator(email: users.Email, name: users.Name)

  final case class Forks(count: ForksCount, maybeParent: Option[ParentProject])

  final case class ParentProject(id: Id, path: Path, name: Name)

  final case class RepoUrls(ssh: SshUrl, http: HttpUrl, web: WebUrl, readme: ReadmeUrl)

  object RepoUrls {

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
