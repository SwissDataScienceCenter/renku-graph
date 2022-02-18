package io.renku.graph.acceptancetests

package object knowledgegraph {

  import io.circe.{Encoder, Json}
  import io.renku.graph.acceptancetests.data.Project.Permissions._
  import io.renku.graph.acceptancetests.data.Project.{Urls, _}
  import io.renku.graph.model.projects.{DateCreated, ForksCount}
  import io.renku.graph.model.testentities
  import io.renku.http.rest.Links.{Href, Link, Rel, _links}
  import io.circe.literal._
  import io.renku.graph.acceptancetests.data.{Project, _}
  import io.renku.graph.model.testentities._
  import eu.timepit.refined.auto._

  def fullJson(project: Project): Json = json"""{
    "identifier":  ${project.id.value}, 
    "path":        ${project.path.value}, 
    "name":        ${project.name.value},
    "visibility":  ${project.entitiesProject.visibility.value},
    "created":     ${(project.entitiesProject.dateCreated, project.entitiesProject.maybeCreator)},
    "updatedAt":   ${project.updatedAt.value},
    "urls":        ${project.urls.toJson},
    "forking":     ${project.entitiesProject.forksCount -> project.entitiesProject},
    "keywords":    ${project.entitiesProject.keywords.map(_.value).toList.sorted},
    "starsCount":  ${project.starsCount.value},
    "permissions": ${toJson(project.permissions)},
    "statistics": {
      "commitsCount":     ${project.statistics.commitsCount.value},
      "storageSize":      ${project.statistics.storageSize.value},
      "repositorySize":   ${project.statistics.repositorySize.value},
      "lfsObjectsSize":   ${project.statistics.lsfObjectsSize.value},
      "jobArtifactsSize": ${project.statistics.jobArtifactsSize.value}
    },
    "version": ${project.entitiesProject.version.value}
  }""" deepMerge {
    _links(
      Link(Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path)),
      Link(Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets"))
    )
  } deepMerge {
    project.entitiesProject.maybeDescription
      .map(description => json"""{"description": ${description.value} }""")
      .getOrElse(Json.obj())
  }

  private implicit class UrlsOps(urls: Urls) {
    import io.renku.json.JsonOps.JsonOps

    lazy val toJson: Json = json"""{
      "ssh":       ${urls.ssh.value},
      "http":      ${urls.http.value},
      "web":       ${urls.web.value}
    }""" addIfDefined ("readme" -> urls.maybeReadme.map(_.value))
  }

  private implicit lazy val forkingEncoder: Encoder[(ForksCount, testentities.RenkuProject)] =
    Encoder.instance {
      case (forksCount, project: testentities.RenkuProject.WithParent) => json"""{
      "forksCount": ${forksCount.value},
      "parent": {
        "path":    ${project.parent.path.value},
        "name":    ${project.parent.name.value},
        "created": ${(project.parent.dateCreated, project.parent.maybeCreator)}
      }
    }"""
      case (forksCount, _) => json"""{
      "forksCount": ${forksCount.value}
    }"""
    }

  private implicit lazy val createdEncoder: Encoder[(DateCreated, Option[Person])] = Encoder.instance {
    case (dateCreated, Some(creator)) => json"""{
      "dateCreated": ${dateCreated.value},
      "creator": $creator
    }"""
    case (dateCreated, _) => json"""{
      "dateCreated": ${dateCreated.value}
    }"""
  }

  private implicit lazy val personEncoder: Encoder[Person] = Encoder.instance {
    case Person(name, Some(email), _, _) => json"""{
      "name": ${name.value},
      "email": ${email.value}
    }"""
    case Person(name, _, _, _) => json"""{
      "name": ${name.value}
    }"""
  }

  private lazy val toJson: Permissions => Json = {
    case ProjectAndGroupPermissions(projectAccessLevel, groupAccessLevel) => json"""{
      "projectAccess": {
        "level": {"name": ${projectAccessLevel.name.value}, "value": ${projectAccessLevel.value.value}}
      },
      "groupAccess": {
        "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
      }
    }"""
    case ProjectPermissions(projectAccessLevel) => json"""{
      "projectAccess": {
        "level": {"name": ${projectAccessLevel.name.value}, "value": ${projectAccessLevel.value.value}}
      }
    }"""
    case GroupPermissions(groupAccessLevel) => json"""{
      "groupAccess": {
        "level": {"name": ${groupAccessLevel.name.value}, "value": ${groupAccessLevel.value.value}}
      }
    }"""
  }
}
