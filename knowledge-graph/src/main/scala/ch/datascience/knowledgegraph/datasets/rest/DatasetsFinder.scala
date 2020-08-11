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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate, Title}
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.knowledgegraph.datasets.model.DatasetPublishing
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(maybePhrase: Option[Phrase],
                   sort:        Sort.By,
                   paging:      PagingRequest): Interpretation[PagingResponse[DatasetSearchResult]]
}

private object DatasetsFinder {
  final case class DatasetSearchResult(
      id:               Identifier,
      title:            Title,
      name:             Name,
      maybeDescription: Option[Description],
      published:        DatasetPublishing,
      projectsCount:    ProjectsCount
  )

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProjectsCount extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_)) with NonNegativeInt
}

private class IODatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    creatorsFinder:          CreatorsFinder,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with DatasetsFinder[IO]
    with Paging[IO, DatasetSearchResult] {

  import IODatasetsFinder._
  import cats.implicits._
  import creatorsFinder._

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sort.By,
                            pagingRequest: PagingRequest): IO[PagingResponse[DatasetSearchResult]] = {
    implicit val resultsFinder: PagedResultsFinder[IO, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(maybePhrase, sort),
      maybeCountQuery = Some(countQuery(maybePhrase))
    )
    for {
      page                 <- findPage(pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[IO](datasetsWithCreators)
    } yield updatedPage
  }

  private def sparqlQuery(maybePhrase: Option[Phrase], sort: Sort.By): SparqlQuery = SparqlQuery(
    name = queryName(maybePhrase, "ds free-text search"),
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    maybePhrase match {
      case Some(phrase) if phrase.value.trim != "*" =>
        s"""|SELECT ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?projectsCount
            |WHERE {
            |  {
            |    # locating earliest commit (date) where dataset with same origin was added
            |    SELECT ?topmostSameAs (MIN(?dateCreated) AS ?minDateCreated) ?projectsCount
            |    WHERE {
            |      {
            |        # grouping datasets by sameAs and finding number of projects sharing the sameAs
            |        SELECT ?topmostSameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |        WHERE {
            |          {
            |            # finding datasets matching the phrase 
            |            # (to narrow down the ds ids set and to prevent from additional lookups to lucene)
            |            SELECT ?topmostSameAs
            |            WHERE {
            |              {
            |                SELECT ?l0
            |                WHERE {
            |                  {
            |                    ?l0 text:query (schema:name '$phrase') ;
            |                        rdf:type <http://schema.org/Dataset> .
            |                  } UNION {
            |                    ?l0 text:query (schema:description '$phrase') ;
            |                        rdf:type <http://schema.org/Dataset> .
            |                  } UNION {
            |                    {
            |                      SELECT ?personId
            |                      WHERE {
            |                        ?personId text:query (schema:name '$phrase') ;
            |                                  rdf:type <http://schema.org/Person> .
            |                      }
            |                      GROUP BY ?personId
            |                    } {
            |                      ?l0 schema:creator ?personId ;
            |                          rdf:type <http://schema.org/Dataset> .
            |                    }
            |                  }
            |                }
            |                GROUP BY ?l0
            |                HAVING (COUNT(*) > 0)
            |              } {
            |                # flattening project the import hierarchy
            |                {
            |                  {
            |                    ?l0 schema:sameAs+/schema:url ?l1.
            |                    FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
            |                    BIND (?l1 AS ?topmostSameAs)
            |                  } UNION {
            |                    ?l0 rdf:type <http://schema.org/Dataset>.
            |                    FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
            |                    BIND (?l0 AS ?topmostSameAs)
            |                  }
            |                } UNION {
            |                  ?l0 schema:sameAs+/schema:url ?l1.
            |                  ?l1 schema:sameAs+/schema:url ?l2.
            |                  FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
            |                  BIND (?l2 AS ?topmostSameAs)
            |                } UNION {
            |                  ?l0 schema:sameAs+/schema:url ?l1.
            |                  ?l1 schema:sameAs+/schema:url ?l2.
            |                  ?l2 schema:sameAs+/schema:url ?l3.
            |                  FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
            |                  BIND (?l3 AS ?topmostSameAs)
            |                } UNION {
            |                  ?l0 schema:sameAs+/schema:url ?l1.
            |                  ?l1 schema:sameAs+/schema:url ?l2.
            |                  ?l2 schema:sameAs+/schema:url ?l3.
            |                  ?l3 schema:sameAs+/schema:url ?l4.
            |                  FILTER NOT EXISTS { ?l4 schema:sameAs ?l5 }
            |                  BIND (?l4 AS ?topmostSameAs)
            |                }
            |              }
            |            }
            |            GROUP BY ?topmostSameAs
            |            HAVING (COUNT(*) > 0)
            |          } {
            |            {
            |              {
            |                ?l0 schema:sameAs+/schema:url ?topmostSameAs;
            |                    schema:isPartOf ?projectId.
            |                FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l2 }
            |              } UNION {
            |                ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                    schema:isPartOf ?projectId.
            |                FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l1 }
            |              }
            |            } UNION {
            |              ?l0 schema:sameAs+/schema:url ?l1;
            |                  schema:isPartOf ?projectId.
            |              ?l1 schema:sameAs+/schema:url ?topmostSameAs
            |              FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l3 }
            |            } UNION {
            |              ?l0 schema:sameAs+/schema:url ?l1;
            |                  schema:isPartOf ?projectId.
            |              ?l1 schema:sameAs+/schema:url ?l2.
            |              ?l2 schema:sameAs+/schema:url ?topmostSameAs
            |              FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l4 }
            |            } UNION {
            |              ?l0 schema:sameAs+/schema:url ?l1;
            |                  schema:isPartOf ?projectId.
            |              ?l1 schema:sameAs+/schema:url ?l2.
            |              ?l2 schema:sameAs+/schema:url ?l3.
            |              ?l3 schema:sameAs+/schema:url ?topmostSameAs
            |              FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l4 }
            |            }
            |          }
            |        }
            |        GROUP BY ?topmostSameAs
            |        HAVING (COUNT(*) > 0)
            |      } {
            |        ?dsId schema:sameAs/schema:url ?topmostSameAs;
            |              prov:qualifiedGeneration/prov:activity ?activityId.
            |        ?activityId prov:startedAtTime ?dateCreated
            |      } UNION {
            |        ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                       schema:identifier ?id;
            |                       prov:qualifiedGeneration/prov:activity ?activityId.
            |        ?activityId prov:startedAtTime ?dateCreated
            |      }
            |    }
            |    GROUP BY ?topmostSameAs ?projectsCount
            |    HAVING (COUNT(*) > 0)
            |  } {
            |    ?dsId schema:sameAs/schema:url ?topmostSameAs;
            |          schema:identifier ?identifier ;
            |          schema:name ?name ;
            |          schema:alternateName ?alternateName ;
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  } UNION {
            |    ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                   schema:identifier ?identifier .
            |    ?dsId schema:identifier ?identifier;
            |          schema:name ?name ;
            |          schema:alternateName ?alternateName ;
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  }
            |}
            |GROUP BY ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?projectsCount
            |HAVING (COUNT(*) > 0)
            |${`ORDER BY`(sort)}
            |""".stripMargin
      case _ =>
        s"""|SELECT ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?projectsCount
            |WHERE {
            |  {
            |    # locating earliest commit (date) where dataset with same origin was added
            |    SELECT ?topmostSameAs (MIN(?dateCreated) AS ?minDateCreated) ?projectsCount
            |    WHERE {
            |      {
            |        # grouping datasets by sameAs and finding number of projects sharing the sameAs 
            |        SELECT ?topmostSameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |        WHERE {
            |          # flattening project the import hierarchy
            |          {
            |            {
            |              ?l0 schema:sameAs+/schema:url ?l1;
            |                  schema:isPartOf ?projectId.
            |              FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
            |              BIND (?l1 AS ?topmostSameAs)
            |            } UNION {
            |              ?l0 rdf:type <http://schema.org/Dataset>;
            |                  schema:isPartOf ?projectId.
            |              FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
            |              BIND (?l0 AS ?topmostSameAs)
            |            }
            |          } UNION {
            |            ?l0 schema:sameAs+/schema:url ?l1;
            |                schema:isPartOf ?projectId.
            |            ?l1 schema:sameAs+/schema:url ?l2
            |            FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
            |            BIND (?l2 AS ?topmostSameAs)
            |          } UNION {
            |            ?l0 schema:sameAs+/schema:url ?l1;
            |                schema:isPartOf ?projectId.
            |            ?l1 schema:sameAs+/schema:url ?l2.
            |            ?l2 schema:sameAs+/schema:url ?l3.
            |            FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
            |            BIND (?l3 AS ?topmostSameAs)
            |          } UNION {
            |            ?l0 schema:sameAs+/schema:url ?l1;
            |                schema:isPartOf ?projectId.
            |            ?l1 schema:sameAs+/schema:url ?l2.
            |            ?l2 schema:sameAs+/schema:url ?l3.
            |            ?l3 schema:sameAs+/schema:url ?l4.
            |            FILTER NOT EXISTS { ?l4 schema:sameAs ?l5 }
            |            BIND (?l4 AS ?topmostSameAs)
            |          }
            |        }
            |        GROUP BY ?topmostSameAs
            |        HAVING (COUNT(*) >0)
            |      } {
            |        ?dsId schema:sameAs/schema:url ?topmostSameAs;
            |              prov:qualifiedGeneration/prov:activity ?activityId .
            |        ?activityId prov:startedAtTime ?dateCreated
            |      } UNION {
            |        ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                       schema:identifier ?id .
            |        ?dsId schema:identifier ?id;
            |              prov:qualifiedGeneration/prov:activity ?activityId .
            |        ?activityId prov:startedAtTime ?dateCreated
            |      }
            |    }
            |    GROUP BY ?topmostSameAs ?projectsCount
            |  } {
            |    ?dsId schema:sameAs/schema:url ?topmostSameAs;
            |          schema:identifier ?identifier ;
            |          schema:name ?name ;
            |          schema:alternateName ?alternateName ;
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  } UNION {
            |    ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                   schema:identifier ?identifier .
            |    ?dsId schema:identifier ?identifier;
            |          schema:name ?name ;
            |          schema:alternateName ?alternateName ;
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  }
            |}
            |GROUP BY ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?projectsCount
            |HAVING (COUNT(*) > 0)
            |${`ORDER BY`(sort)}
            |""".stripMargin
    }
  )

  private def countQuery(maybePhrase: Option[Phrase]): SparqlQuery = SparqlQuery(
    name = queryName(maybePhrase, "ds free-text search - count"),
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    maybePhrase match {
      case Some(phrase) if phrase.value.trim != "*" =>
        s"""|SELECT ?topmostSameAs
            |WHERE {
            |  {
            |    SELECT ?l0
            |    WHERE {
            |      {
            |        ?l0 text:query (schema:name '$phrase') ;
            |            rdf:type <http://schema.org/Dataset> .
            |      } UNION {
            |        ?l0 text:query (schema:description '$phrase') ;
            |            rdf:type <http://schema.org/Dataset> .
            |      } UNION {
            |        {
            |          SELECT ?personId
            |          WHERE {
            |            ?personId text:query (schema:name '$phrase') ;
            |                      rdf:type <http://schema.org/Person> .
            |          }
            |          GROUP BY ?personId
            |        } {
            |          ?l0 schema:creator ?personId ;
            |              rdf:type <http://schema.org/Dataset> .
            |        }
            |      }
            |    }
            |    GROUP BY ?l0
            |    HAVING (COUNT(*) > 0)
            |  } {
            |    {
            |      {
            |        ?l0 schema:sameAs+/schema:url ?l1
            |        FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
            |        BIND (?l1 AS ?topmostSameAs)
            |      } UNION {
            |        ?l0 rdf:type <http://schema.org/Dataset>.
            |        FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
            |        BIND (?l0 AS ?topmostSameAs)
            |      }
            |    } UNION {
            |      ?l0 schema:sameAs+/schema:url ?l1.
            |      ?l1 schema:sameAs+/schema:url ?l2.
            |      FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
            |      BIND (?l2 AS ?topmostSameAs)
            |    } UNION {
            |      ?l0 schema:sameAs+/schema:url ?l1.
            |      ?l1 schema:sameAs+/schema:url ?l2.
            |      ?l2 schema:sameAs+/schema:url ?l3.
            |      FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
            |      BIND (?l3 AS ?topmostSameAs)
            |    } UNION {
            |      ?l0 schema:sameAs+/schema:url ?l1.
            |      ?l1 schema:sameAs+/schema:url ?l2.
            |      ?l2 schema:sameAs+/schema:url ?l3.
            |      ?l3 schema:sameAs+/schema:url ?l4.
            |      FILTER NOT EXISTS { ?l4 schema:sameAs ?l5 }
            |      BIND (?l4 AS ?topmostSameAs)
            |    }
            |  }
            |}
            |GROUP BY ?topmostSameAs
            |""".stripMargin
      case _ =>
        s"""|SELECT ?topmostSameAs
            |WHERE {
            |  {
            |    {
            |      ?l0 schema:sameAs+/schema:url ?l1
            |      FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
            |      BIND (?l1 AS ?topmostSameAs)
            |    } UNION {
            |      ?l0 rdf:type <http://schema.org/Dataset>.
            |      FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
            |      BIND (?l0 AS ?topmostSameAs)
            |    }
            |  } UNION {
            |    ?l0 schema:sameAs+/schema:url ?l1.
            |    ?l1 schema:sameAs+/schema:url ?l2.
            |    FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
            |    BIND (?l2 AS ?topmostSameAs)
            |  } UNION {
            |    ?l0 schema:sameAs+/schema:url ?l1.
            |    ?l1 schema:sameAs+/schema:url ?l2.
            |    ?l2 schema:sameAs+/schema:url ?l3.
            |    FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
            |    BIND (?l3 AS ?topmostSameAs)
            |  } UNION {
            |    ?l0 schema:sameAs+/schema:url ?l1.
            |    ?l1 schema:sameAs+/schema:url ?l2.
            |    ?l2 schema:sameAs+/schema:url ?l3.
            |    ?l3 schema:sameAs+/schema:url ?l4.
            |    FILTER NOT EXISTS { ?l4 schema:sameAs ?l5 }
            |    BIND (?l4 AS ?topmostSameAs)
            |  }
            |}
            |GROUP BY ?topmostSameAs
            |""".stripMargin
    }
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.TitleProperty         => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }

  private lazy val addCreators: DatasetSearchResult => IO[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(published = dataset.published.copy(creators = creators)))

  private def queryName(maybePhrase: Option[Phrase], name: String Refined NonEmpty): String Refined NonEmpty =
    maybePhrase match {
      case Some(phrase) if phrase.value.trim != "*" => name
      case _                                        => Refined.unsafeApply(s"$name *")
    }
}

private object IODatasetsFinder {
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.ProjectsCount
  import io.circe.Decoder

  implicit val recordDecoder: Decoder[DatasetSearchResult] = { cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      id                 <- cursor.downField("identifier").downField("value").as[Identifier]
      title              <- cursor.downField("name").downField("value").as[Title]
      name               <- cursor.downField("alternateName").downField("value").as[Name]
      maybePublishedDate <- cursor.downField("maybePublishedDate").downField("value").as[Option[PublishedDate]]
      projectsCount      <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      maybeDescription <- cursor
                           .downField("maybeDescription")
                           .downField("value")
                           .as[Option[String]]
                           .map(blankToNone)
                           .flatMap(toOption[Description])
    } yield DatasetSearchResult(
      id,
      title,
      name,
      maybeDescription,
      DatasetPublishing(maybePublishedDate, Set.empty),
      projectsCount
    )
  }
}
