package io.renku.knowledgegraph.projects.images

import cats.syntax.all._
import io.circe.{Encoder, Json}
import io.circe.literal._
import io.renku.graph.model.{GitLabUrl, projects}
import io.renku.graph.model.images.ImageUri
import io.renku.http.rest.Links.{Href, Link, Rel, _links}

trait ImagesEncoder {
  implicit def imagesEncoder(implicit gitLabUrl: GitLabUrl): Encoder[(List[ImageUri], projects.Path)] =
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
}
