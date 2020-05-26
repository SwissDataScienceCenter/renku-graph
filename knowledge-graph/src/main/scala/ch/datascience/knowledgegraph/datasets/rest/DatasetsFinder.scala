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
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate}
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.knowledgegraph.datasets.model.DatasetPublishing
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore._
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
        s"""|SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
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
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  } UNION {
            |    ?topmostSameAs rdf:type <http://schema.org/Dataset>;
            |                   schema:identifier ?identifier .
            |    ?dsId schema:identifier ?identifier;
            |          schema:name ?name ;
            |          prov:qualifiedGeneration/prov:activity ?activityId .
            |    ?activityId prov:startedAtTime ?minDateCreated
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |  }
            |}
            |GROUP BY ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
            |HAVING (COUNT(*) > 0)
            |${`ORDER BY`(sort)}
            |""".stripMargin
      case _ =>
        s"""|$nonPhraseQuery
            |${`ORDER BY`(sort)}
            |""".stripMargin
    }
  )

  private def countQuery(maybePhrase: Option[Phrase]): SparqlQuery = SparqlQuery(
    name = queryName(maybePhrase, "ds free-text search - count"),
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
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
        """|SELECT ?topmostDsId ?groupingIdentifier
           |WHERE {
           |  { # finding all ds having neither sameAs nor wasDerivedFrom
           |    ?dsId rdf:type <http://schema.org/Dataset>;
           |          schema:isPartOf ?projectId.
           |    FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
           |    FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
           |    FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
           |    FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
           |    BIND (?dsId AS ?topmostDsId)
           |    BIND (?dsId AS ?groupingIdentifier)
           |    BIND (3 AS ?case)
           |  } UNION { # finding all ds with the import hierarchy
           |    SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (2 AS ?case)
           |    WHERE {
           |      {
           |        {
           |          SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
           |          WHERE {
           |            {
           |              SELECT (?childId AS ?topmostSameAs)
           |              WHERE {
           |                {
           |                  SELECT DISTINCT ?sameAs
           |                  WHERE {
           |                  ?someDsId rdf:type <http://schema.org/Dataset>;
           |                            schema:sameAs/schema:url ?sameAs.
           |                  }
           |                } {
           |                  ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
           |                  ?childId (schema:sameAs/schema:url)* ?parentDsId.
           |                }
           |              }
           |              GROUP BY ?childId
           |              HAVING (count(?parentDsId) = 1)
           |            } {
           |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
           |                    prov:qualifiedGeneration/prov:activity ?activityId .
           |              ?activityId prov:startedAtTime ?dateCreated
           |            } UNION {
           |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
           |                             schema:identifier ?id .
           |              ?dsId schema:identifier ?id;
           |                    prov:qualifiedGeneration/prov:activity ?activityId .
           |              ?activityId prov:startedAtTime ?dateCreated
           |            }
           |          }
           |          GROUP BY ?topmostSameAs
           |          HAVING (COUNT(*) > 0)
           |        } {
           |          ?dsId schema:sameAs/schema:url ?topmostSameAs;
           |                prov:qualifiedGeneration/prov:activity ?activityId.
           |          ?activityId prov:startedAtTime ?minDateCreated
           |          BIND (?topmostSameAs AS ?sameAs)
           |          BIND (?dsId AS ?topmostDsId)
           |        } UNION {
           |          ?topmostSameAs rdf:type <http://schema.org/Dataset>;
           |                         schema:identifier ?identifier .
           |          ?dsId schema:identifier ?identifier;
           |                prov:qualifiedGeneration/prov:activity ?activityId.
           |          ?activityId prov:startedAtTime ?minDateCreated
           |          BIND (?topmostSameAs AS ?sameAs)
           |          BIND (?topmostSameAs AS ?topmostDsId)
           |        }
           |      } {
           |        ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
           |        ?childId (schema:sameAs/schema:url)* ?parentDsId.
           |      } {
           |        ?foundDsId rdf:type <http://schema.org/Dataset>;
           |                   schema:sameAs/schema:url ?sameAs;
           |                   schema:isPartOf ?projectId.
           |      } UNION {
           |        ?sameAs rdf:type <http://schema.org/Dataset>;
           |                schema:isPartOf ?projectId.
           |        BIND (?sameAs AS ?foundDsId)
           |      }
           |      # getting rid of those ds which are either deriving or being derived
           |      FILTER NOT EXISTS { 
           |        ?foundDsId prov:wasDerivedFrom ?ds1Id.
           |        FILTER EXISTS { ?ds1Id schema:isPartOf ?projectId }
           |      }
           |      FILTER NOT EXISTS { 
           |        ?ds2Id prov:wasDerivedFrom ?foundDsId.
           |        FILTER EXISTS { ?ds2Id schema:isPartOf ?projectId }
           |      }
           |    }
           |  } UNION { # finding all ds with the modification hierarchy
           |    SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (1 AS ?case)
           |    WHERE {
           |      {
           |        SELECT (?childId AS ?dsId) ?topmostDsId
           |        WHERE {
           |          {
           |            SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
           |            WHERE {
           |              SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
           |              WHERE {
           |                {
           |                  SELECT (?childId AS ?topmostDsId)
           |                  WHERE {
           |                    {
           |                      SELECT DISTINCT ?dsIdToCheck
           |                      WHERE {
           |                        ?someDsId rdf:type <http://schema.org/Dataset>;
           |                                  prov:wasDerivedFrom ?dsIdToCheck.
           |                      }
           |                    } {
           |                      ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
           |                      ?childId prov:wasDerivedFrom* ?parentDsId.
           |                    }
           |                  }
           |                  GROUP BY ?childId
           |                  HAVING (count(?parentDsId) = 1)
           |                } {
           |                  ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
           |                  ?childId prov:wasDerivedFrom* ?parentDsId.
           |                }
           |              }
           |              GROUP BY ?topmostDsId ?childId
           |            }
           |            GROUP BY ?topmostDsId
           |          } {
           |              ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
           |              ?childId prov:wasDerivedFrom* ?parentDsId.
           |          }
           |        }
           |        GROUP BY ?topmostDsId ?childId ?maxDepth
           |        HAVING (count(?parentDsId) - 1 = ?maxDepth)
           |      } {
           |        ?dsId rdf:type <http://schema.org/Dataset>;
           |              schema:isPartOf ?projectId.
           |      }
           |    }
           |  }
           |}
           |GROUP BY ?topmostDsId ?groupingIdentifier ?case
           |""".stripMargin
    }
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.NameProperty          => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }

  private lazy val nonPhraseQuery =
    """|SELECT DISTINCT ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs ?projectsCount
       |WHERE {
       |  {
       |    SELECT ?topmostDsId ?groupingIdentifier (COUNT(DISTINCT ?projectId) AS ?projectsCount) (?case AS ?recordCase)
       |    WHERE {
       |      { # finding all ds having neither sameAs nor wasDerivedFrom
       |        ?dsId rdf:type <http://schema.org/Dataset>;
       |              schema:isPartOf ?projectId.
       |        FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
       |        FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
       |        FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
       |        FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
       |        BIND (?dsId AS ?topmostDsId)
       |        BIND (?dsId AS ?groupingIdentifier)
       |        BIND (3 AS ?case)
       |      } UNION { # finding all ds with the import hierarchy
       |        SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (2 AS ?case)
       |        WHERE {
       |          {
       |            {
       |              SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
       |              WHERE {
       |                {
       |                  SELECT (?childId AS ?topmostSameAs)
       |                  WHERE {
       |                    {
       |                      SELECT DISTINCT ?sameAs
       |                      WHERE {
       |                      ?someDsId rdf:type <http://schema.org/Dataset>;
       |                                schema:sameAs/schema:url ?sameAs.
       |                      }
       |                    } {
       |                      ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
       |                      ?childId (schema:sameAs/schema:url)* ?parentDsId.
       |                    }
       |                  }
       |                  GROUP BY ?childId
       |                  HAVING (count(?parentDsId) = 1)
       |                } {
       |                  ?dsId schema:sameAs/schema:url ?topmostSameAs;
       |                        prov:qualifiedGeneration/prov:activity ?activityId .
       |                  ?activityId prov:startedAtTime ?dateCreated
       |                } UNION {
       |                  ?topmostSameAs rdf:type <http://schema.org/Dataset>;
       |                                 schema:identifier ?id .
       |                  ?dsId schema:identifier ?id;
       |                        prov:qualifiedGeneration/prov:activity ?activityId .
       |                  ?activityId prov:startedAtTime ?dateCreated
       |                }
       |              }
       |              GROUP BY ?topmostSameAs
       |              HAVING (COUNT(*) > 0)
       |            } {
       |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
       |                    prov:qualifiedGeneration/prov:activity ?activityId.
       |              ?activityId prov:startedAtTime ?minDateCreated
       |              BIND (?topmostSameAs AS ?sameAs)
       |              BIND (?dsId AS ?topmostDsId)
       |            } UNION {
       |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
       |                             schema:identifier ?identifier .
       |              ?dsId schema:identifier ?identifier;
       |                    prov:qualifiedGeneration/prov:activity ?activityId.
       |              ?activityId prov:startedAtTime ?minDateCreated
       |              BIND (?topmostSameAs AS ?sameAs)
       |              BIND (?topmostSameAs AS ?topmostDsId)
       |            }
       |          } {
       |            ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
       |            ?childId (schema:sameAs/schema:url)* ?parentDsId.
       |          } {
       |            ?foundDsId rdf:type <http://schema.org/Dataset>;
       |                       schema:sameAs/schema:url ?sameAs;
       |                       schema:isPartOf ?projectId.
       |          } UNION {
       |            ?sameAs rdf:type <http://schema.org/Dataset>;
       |                    schema:isPartOf ?projectId.
       |            BIND (?sameAs AS ?foundDsId)
       |          } 
       |          # getting rid of those ds which are either deriving or being derived
       |          FILTER NOT EXISTS { 
       |            ?foundDsId prov:wasDerivedFrom ?ds1Id.
       |            FILTER EXISTS { ?ds1Id schema:isPartOf ?projectId }
       |          }
       |          FILTER NOT EXISTS { 
       |            ?ds2Id prov:wasDerivedFrom ?foundDsId.
       |            FILTER EXISTS { ?ds2Id schema:isPartOf ?projectId }
       |          }
       |        }
       |      } UNION { # finding all ds with the modification hierarchy
       |        SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (1 AS ?case)
       |        WHERE {
       |          {
       |            SELECT (?childId AS ?dsId) ?topmostDsId
       |            WHERE {
       |              {
       |                SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
       |                WHERE {
       |                  SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
       |                  WHERE {
       |                    {
       |                      SELECT (?childId AS ?topmostDsId)
       |                      WHERE {
       |                        {
       |                          SELECT DISTINCT ?dsIdToCheck
       |                          WHERE {
       |                            ?someDsId rdf:type <http://schema.org/Dataset>;
       |                                      prov:wasDerivedFrom ?dsIdToCheck.
       |                          }
       |                        } {
       |                          ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
       |                          ?childId prov:wasDerivedFrom* ?parentDsId.
       |                        }
       |                      }
       |                      GROUP BY ?childId
       |                      HAVING (count(?parentDsId) = 1)
       |                    } {
       |                      ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
       |                      ?childId prov:wasDerivedFrom* ?parentDsId.
       |                    }
       |                  }
       |                  GROUP BY ?topmostDsId ?childId
       |                }
       |                GROUP BY ?topmostDsId
       |              } {
       |                  ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
       |                  ?childId prov:wasDerivedFrom* ?parentDsId.
       |              }
       |            }
       |            GROUP BY ?topmostDsId ?childId ?maxDepth
       |            HAVING (count(?parentDsId) - 1 = ?maxDepth)
       |          } {
       |            ?dsId rdf:type <http://schema.org/Dataset>;
       |                  schema:isPartOf ?projectId.
       |          }
       |        }
       |      }
       |    }
       |    GROUP BY ?topmostDsId ?groupingIdentifier ?case
       |  }
       |  FILTER (?topmostDsIdInner = ?topmostDsId && ?caseInner = ?recordCase) {
       |    SELECT (?topmostDsId AS ?topmostDsIdInner) ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (?case AS ?caseInner)
       |    WHERE {
       |      { # finding all ds having neither sameAs nor wasDerivedFrom
       |            ?dsId rdf:type <http://schema.org/Dataset>.
       |            FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
       |            FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
       |            FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
       |            FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
       |            ?dsId rdf:type <http://schema.org/Dataset>;
       |                  schema:identifier ?identifier;
       |                  schema:name ?name.
       |            OPTIONAL { ?dsId schema:description ?maybeDescription }
       |            OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
       |            OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
       |            OPTIONAL { ?dsId schema:url ?maybeUrl }
       |            BIND (IF (BOUND(?maybeUrl), ?maybeUrl, ?dsId) as ?sameAs)
       |            BIND (?dsId AS ?topmostDsId)
       |            BIND (3 AS ?case)
       |      } UNION { # finding all ds with the import hierarchy
       |        SELECT DISTINCT ?topmostDsId ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (2 AS ?case)
       |        WHERE {
       |          {
       |            {
       |              SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
       |              WHERE {
       |                {
       |                  SELECT (?childId AS ?topmostSameAs)
       |                  WHERE {
       |                    {
       |                      SELECT DISTINCT ?sameAs
       |                      WHERE {
       |                      ?someDsId rdf:type <http://schema.org/Dataset>;
       |                                schema:sameAs/schema:url ?sameAs.
       |                      }
       |                    } {
       |                      ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
       |                      ?childId (schema:sameAs/schema:url)* ?parentDsId.
       |                    }
       |                  }
       |                  GROUP BY ?childId
       |                  HAVING (count(?parentDsId) = 1)
       |                } {
       |                  ?dsId schema:sameAs/schema:url ?topmostSameAs;
       |                        prov:qualifiedGeneration/prov:activity ?activityId .
       |                  ?activityId prov:startedAtTime ?dateCreated
       |                } UNION {
       |                  ?topmostSameAs rdf:type <http://schema.org/Dataset>;
       |                                 schema:identifier ?id .
       |                  ?dsId schema:identifier ?id;
       |                        prov:qualifiedGeneration/prov:activity ?activityId .
       |                  ?activityId prov:startedAtTime ?dateCreated
       |                }
       |              }
       |              GROUP BY ?topmostSameAs
       |            } {
       |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
       |                    schema:identifier ?identifier;
       |                    schema:name ?name;
       |                    prov:qualifiedGeneration/prov:activity ?activityId.
       |              ?activityId prov:startedAtTime ?minDateCreated
       |              OPTIONAL { ?dsId schema:description ?maybeDescription }
       |              OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
       |              OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
       |              OPTIONAL { ?dsId schema:url ?maybeUrl }
       |              BIND (?topmostSameAs AS ?sameAs)
       |              BIND (?dsId AS ?topmostDsId)
       |            } UNION {
       |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
       |                             schema:identifier ?identifier .
       |              ?dsId schema:identifier ?identifier;
       |                    schema:name ?name;
       |                    prov:qualifiedGeneration/prov:activity ?activityId.
       |              ?activityId prov:startedAtTime ?minDateCreated
       |              OPTIONAL { ?dsId schema:description ?maybeDescription }
       |              OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
       |              OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
       |              OPTIONAL { ?dsId schema:url ?maybeUrl }
       |              BIND (?topmostSameAs AS ?sameAs)
       |              BIND (?topmostSameAs AS ?topmostDsId)
       |            }
       |          }
       |        }
       |      }  UNION { # finding all ds with the modification hierarchy
       |        SELECT ?topmostDsId ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (1 AS ?case)
       |        WHERE {
       |          {
       |            SELECT (?childId AS ?dsId) ?topmostDsId
       |            WHERE {
       |              {
       |                SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
       |                WHERE {
       |                  SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
       |                  WHERE {
       |                    {
       |                      SELECT (?childId AS ?topmostDsId)
       |                      WHERE {
       |                        {
       |                          SELECT DISTINCT ?dsIdToCheck
       |                          WHERE {
       |                            ?someDsId rdf:type <http://schema.org/Dataset>;
       |                                      prov:wasDerivedFrom ?dsIdToCheck.
       |                          }
       |                        } {
       |                          ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
       |                          ?childId prov:wasDerivedFrom* ?parentDsId.
       |                        }
       |                      }
       |                      GROUP BY ?childId
       |                      HAVING (count(?parentDsId) = 1)
       |                    } {
       |                      ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
       |                      ?childId prov:wasDerivedFrom* ?parentDsId.
       |                    }
       |                  }
       |                  GROUP BY ?topmostDsId ?childId
       |                }
       |                GROUP BY ?topmostDsId
       |              } {
       |                  ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
       |                  ?childId prov:wasDerivedFrom* ?parentDsId.
       |              }
       |            }
       |            GROUP BY ?topmostDsId ?childId ?maxDepth
       |            HAVING (count(?parentDsId) - 1 = ?maxDepth)
       |          } {
       |            ?dsId rdf:type <http://schema.org/Dataset>;
       |                  schema:identifier ?identifier;
       |                  schema:name ?name.
       |            OPTIONAL { ?dsId schema:description ?maybeDescription }
       |            OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
       |            OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
       |            OPTIONAL { ?dsId schema:url ?maybeUrl }
       |            BIND (IF (BOUND(?maybeUrl), ?maybeUrl, ?dsId) as ?sameAs)
       |          }
       |        }
       |      }
       |    }
       |  }
       |}
       |""".stripMargin

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
      name               <- cursor.downField("name").downField("value").as[Name]
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
      name,
      maybeDescription,
      DatasetPublishing(maybePublishedDate, Set.empty),
      projectsCount
    )
  }
}
