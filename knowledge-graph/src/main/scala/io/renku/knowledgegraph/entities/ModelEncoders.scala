package io.renku.knowledgegraph.entities

import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.config.renku
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.http.rest.Links.{Href, Link, Rel, _links}
import io.renku.json.JsonOps._
import io.renku.knowledgegraph

private object ModelEncoders {

  implicit def projectEncoder(implicit renkuApiUrl: renku.ApiUrl): Encoder[model.Entity.Project] =
    Encoder.instance { project =>
      json"""{
      "type":          ${Criteria.Filters.EntityType.Project.value},
      "matchingScore": ${project.matchingScore},
      "name":          ${project.name},
      "path":          ${project.path},
      "namespace":     ${project.path.toNamespaces.mkString("/")},
      "namespaces":    ${toDetailedInfo(project.path.toNamespaces)},
      "visibility":    ${project.visibility},
      "date":          ${project.date},
      "keywords":      ${project.keywords}
    }"""
        .addIfDefined("creator" -> project.maybeCreator)
        .addIfDefined("description" -> project.maybeDescription)
        .deepMerge(
          _links(
            Link(Rel("details") -> knowledgegraph.projects.details.Endpoint.href(renkuApiUrl, project.path))
          )
        )
    }

  private type NamespaceInfo = (Rel, List[projects.Namespace])

  private lazy val toDetailedInfo: List[projects.Namespace] => Json = _.foldLeft(List.empty[NamespaceInfo]) {
    case (Nil, namespace) => List(Rel(namespace.show) -> List(namespace))
    case (all @ (_, lastNamespaces) :: _, namespace) =>
      all ::: (Rel(namespace.show) -> (lastNamespaces ::: namespace :: Nil)) :: Nil
  }.asJson

  private implicit lazy val namespaceEncoder: Encoder[NamespaceInfo] = Encoder.instance { case (rel, namespaces) =>
    json"""{
      "rel":       $rel,
      "namespace": ${namespaces.map(_.show).mkString("/")}
    }"""
  }

  implicit def datasetEncoder(implicit
      gitLabUrl:   GitLabUrl,
      renkuApiUrl: renku.ApiUrl
  ): Encoder[model.Entity.Dataset] = {

    implicit lazy val imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
      Encoder.instance[(List[ImageUri], projects.Path)] { case (imageUris, exemplarProjectPath) =>
        Json.arr(imageUris.map {
          case uri: ImageUri.Relative =>
            json"""{
              "location": $uri
            }""" deepMerge _links(
              Link(Rel("view") -> Href(gitLabUrl / exemplarProjectPath / "raw" / "master" / uri))
            )
          case uri: ImageUri.Absolute =>
            json"""{
              "location": $uri
            }""" deepMerge _links(Link(Rel("view") -> Href(uri.show)))
        }: _*)
      }

    Encoder.instance { ds =>
      json"""{
        "type":          ${Criteria.Filters.EntityType.Dataset.value},
        "matchingScore": ${ds.matchingScore},
        "name":          ${ds.name},
        "visibility":    ${ds.visibility},
        "date":          ${ds.date},
        "creators":      ${ds.creators},
        "keywords":      ${ds.keywords},
        "images":        ${(ds.images -> ds.exemplarProjectPath).asJson}
      }"""
        .addIfDefined("description" -> ds.maybeDescription)
        .deepMerge(
          _links(
            Link(Rel("details") -> knowledgegraph.datasets.details.Endpoint.href(renkuApiUrl, ds.identifier))
          )
        )
    }
  }

  implicit lazy val workflowEncoder: Encoder[model.Entity.Workflow] =
    Encoder.instance { workflow =>
      json"""{
      "type":          ${Criteria.Filters.EntityType.Workflow.value},
      "matchingScore": ${workflow.matchingScore},
      "name":          ${workflow.name},
      "visibility":    ${workflow.visibility},
      "date":          ${workflow.date},
      "keywords":      ${workflow.keywords},
      "workflowType": ${workflow.workflowType}
    }"""
        .addIfDefined("description" -> workflow.maybeDescription)
    }

  implicit lazy val personEncoder: Encoder[model.Entity.Person] =
    Encoder.instance { person =>
      json"""{
      "type":          ${Criteria.Filters.EntityType.Person.value},
      "matchingScore": ${person.matchingScore},
      "name":          ${person.name}
    }"""
    }
}
