package io.renku.graph.acceptancetests.knowledgegraph

import io.circe.{Encoder, Json}
import io.circe.literal._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.{GitLabUrl, projects}

trait ImageApiEncoders {
  def gitLabUrl: GitLabUrl

  implicit def imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (images, exemplarProjectPath) =>
      Json.arr(images.map {
        case uri: ImageUri.Relative => json"""{
           "_links": [{
             "rel": "view",
             "href": ${s"$gitLabUrl/$exemplarProjectPath/raw/master/$uri"}
           }],
           "location": $uri
         }"""
        case uri: ImageUri.Absolute => json"""{
           "_links": [{
             "rel": "view",
             "href": $uri
           }],
           "location": $uri
         }"""
      }: _*)
    }
}
