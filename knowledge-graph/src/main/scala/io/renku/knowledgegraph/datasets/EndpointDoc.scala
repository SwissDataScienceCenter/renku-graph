package io.renku.knowledgegraph.datasets

import cats.implicits._
import io.circe.literal._
import io.renku.knowledgegraph.docs.Implicits._
import io.renku.knowledgegraph.docs.model.Example.JsonExample
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model.Path.OpMapping
import io.renku.knowledgegraph.docs.model._
import org.http4s

object EndpointDoc {

  lazy val paths: List[Path] = List(SingleDataset.path, DatasetSearch.path)

  private object SingleDataset {
    private[datasets] lazy val path = Path(
      "Datasets",
      "Finds details of the dataset with the given id.".some,
      GET(
        Uri / "datasets" / idParam,
        http4s.Status.Ok.asDocStatus,
        Response(
          "Lineage found",
          Map(
            "json" -> MediaType(http4s.MediaType.application.json.asDocMediaType,
                                "Sample Lineage",
                                JsonExample(example)
            )
          )
        )
      )
    )

    private lazy val idParam = Parameter.in(
      "id",
      Schema.String,
      description = "Dataset IDs".some
    )

    private lazy val example = json"""{
                                        "_links" : [
                                          {
                                            "rel" : "self",
                                            "href" : "https://zemdgsw:9540/datasets/22222222-2222-2222-2222-222222222222"
                                          },
                                          {
                                            "rel" : "initial-version",
                                            "href" : "https://zemdgsw:9540/datasets/11111111-1111-1111-1111-111111111111"
                                          }
                                        ],
                                        "identifier" : "22222222-2222-2222-2222-222222222222",
                                        "versions" : {
                                          "initial": "11111111-1111-1111-1111-111111111111"
                                        },
                                        "title" : "dataset title",
                                        "name" : "dataset alternate name",
                                        "url" : "http://host/url1",                     
                                        "sameAs" : "http://host/url2",                  
                                        "derivedFrom" : "http://host/url1",             
                                        "description" : "vbnqyyjmbiBQpubavGpxlconuqj",  
                                        "published" : {
                                          "datePublished" : "2012-10-14",               
                                          "creator" : [
                                            {
                                              "name" : "e wmtnxmcguz"
                                              "affiliation" : "SDSC"                    
                                            },
                                            {
                                              "name" : "iilmadw vcxabmh",
                                              "email" : "ticUnrW@cBmrdomoa"             
                                            }
                                          ]
                                        },
                                        "created" : "2012-10-15T03:02:25.639Z",         
                                        "hasPart" : [
                                          {
                                            "atLocation" : "data/dataset-name/file1"
                                          },
                                          {
                                            "atLocation" : "data/dataset-name/file2"
                                          }
                                        ],
                                        "project":  {
                                          "_links" : [
                                            {
                                              "rel" : "project-details",
                                              "href" : "https://zemdgsw:9540/projects/namespace1/project1-name"
                                            }
                                          ],
                                          "path" : "namespace1/project1-name",
                                          "name" : "project1 name"
                                        },
                                        "usedIn" : [
                                          {
                                            "_links" : [
                                              {
                                                "rel" : "project-details",
                                                "href" : "https://zemdgsw:9540/projects/namespace1/project1-name"
                                              }
                                            ],
                                            "path" : "namespace1/project1-name",
                                            "name" : "project1 name"
                                          },
                                          {
                                            "_links" : [
                                              {
                                                "rel" : "project-details",
                                                "href" : "https://zemdgsw:9540/projects/namespace2/project2-name"
                                              }
                                            ],
                                            "path" : "namespace2/project2-name",
                                            "name" : "project2 name"
                                          }
                                        ],
                                        "keywords": [ "rldzpwo", "gfioui" ],
                                        "images": [
                                          {
                                            "location": "image.png",
                                            "_links":[  
                                               {  
                                                  "rel":  "view",
                                                  "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
                                               }
                                            ]
                                          },
                                          {
                                            "location": "http://host/external-image.png",
                                            "_links":[  
                                               {  
                                                  "rel":  "view",
                                                  "href": "http://host/external-image.png"
                                               }
                                            ]
                                          }
                                        ],
                                      }"""
  }

  private object DatasetSearch {
    private[datasets] lazy val path =
      Path("Datasets", "Finds details of the dataset with the given id.".some, opMapping)

  }
  private lazy val opMapping: OpMapping = OpMapping(template, operation)
  private lazy val uri       = Uri / "datasets"
  private lazy val template  = Uri.getTemplate(uri.parts)
  private lazy val operation = Operation.Get("Get datasets matching query".some, parameters, None, responses, Nil)
  private lazy val parameters = List(
    Parameter.in(
      "query",
      Schema.String,
      description = "Query string".some
    ),
    Parameter.in(
      "sort",
      Schema.String,
      description =
        "the sort query parameter is optional and defaults to title:asc. Allowed property names are: title, datePublished, date and projectsCount.".some
    ),
    Parameter.in(
      "page",
      Schema.String,
      description = "the page query parameter is optional and defaults to 1".some
    ),
    Parameter.in(
      "per_page",
      Schema.String,
      description = "the per_page query parameter is optional and defaults to 20".some
    )
  )

  private lazy val responses = Map(
    http4s.Status.Ok.asDocStatus -> Response(
      "OK",
      Map(
        "json" -> MediaType(http4s.MediaType.application.json.asDocMediaType, "Sample datasets", JsonExample(example))
      ),
      responseHeaders,
      Map.empty
    )
  )

  private lazy val responseHeaders = Map(
    "Total"       -> Header("The total number of datasets found".some, Schema.Integer),
    "Total-Pages" -> Header("The total number of pages".some, Schema.Integer),
    "Per-Page"    -> Header("The number of items per page".some, Schema.Integer),
    "Page"        -> Header("The index of the current page (starting at 1)".some, Schema.Integer),
    "Next-Page"   -> Header("The index of the next page (optional)".some, Schema.Integer),
    "Prev-Page"   -> Header("The index of the previous page (optional)".some, Schema.Integer),
    "Link" -> Header("The set of prev/next/first/last link headers (prev and next are optional)".some, Schema.String)
  )

  private lazy val example =
    json"""
      [
   {  
      "identifier": "9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c",
      "title":"rmDaYfpehl",
      "name": "mniouUnmal",
      "description": "vbnqyyjmbiBQpubavGpxlconuqj",  
      "published": {
        "datePublished": "2012-10-14", 
        "creator": [
          {
            "name": "e wmtnxmcguz"
          },
          {
            "name": "iilmadw vcxabmh",
            "email": "ticUnrW@cBmrdomoa"            
          }
        ]
      },
      "date": "2012-10-14T03:02:25.639Z",           
      "projectsCount": 2,
      "keywords": ["grüezi", "안녕", "잘 지내?"],
      "images": [
        {
          "location": "image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://renkulab.io/gitlab/project_path/raw/master/data/mniouUnmal/image.png"
             }
          ]
        }
      ],
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/9f94add6-6d68-4cf4-91d9-4ba9e6b7dc4c"
         }
      ]
   },
   {  
      "identifier": "a1b1cb86-c664-4250-a1e3-578a8a22dcbb",
      "name": "a",
      "published": {
        "creator": [
          {
            "name": "e wmtnxmcguz"
          }
        ]
      },
      "date": "2012-11-15T10:00:00.000Z",            
      "projectsCount": 1,
      "keywords": [],
      "images": [
        {
          "location": "https://blah.com/image.png",
          "_links":[  
             {  
                "rel":  "view",
                "href": "https://blah.com/image.png"
             }
          ]
        }
      ],
      "_links":[  
         {  
            "rel":"details",
            "href":"http://t:5511/datasets/a1b1cb86-c664-4250-a1e3-578a8a22dcbb"
         }
      ]
   }
]
        """

}
