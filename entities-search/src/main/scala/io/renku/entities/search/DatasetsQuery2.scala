package io.renku.entities.search

import cats.syntax.all._
import io.circe.Decoder
import io.renku.entities.search.Criteria.Filters
import io.renku.graph.model.{persons, projects}
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import model.Entity

object DatasetsQuery2 extends EntityQuery[Entity.Dataset] {
  override val entityType: Filters.EntityType = Filters.EntityType.Dataset

  val entityTypeVar           = VarName("entityType")
  val matchingScoreVar        = VarName("matchingScore")
  val nameVar                 = VarName("name")
  val idsPathsVisibilitiesVar = VarName("idsPathsVisibilitiesVar")
  val sameAsVar               = VarName("sameAs")
  val maybeDateCreatedVar     = VarName("maybeDateCreated")
  val maybeDatePublishedVar   = VarName("maybeDatePublished")
  val dateVar                 = VarName("date")
  val creatorsNamesVar        = VarName("creatorsNames")
  val maybeDescriptionVar     = VarName("maybeDescription")
  val keywordsVar             = VarName("keywords")
  val imagesVar               = VarName("images")
  val dsId                    = VarName("id")

  override val selectVariables = Set(
    entityTypeVar,
    matchingScoreVar,
    nameVar,
    idsPathsVisibilitiesVar,
    sameAsVar,
    maybeDateCreatedVar,
    maybeDatePublishedVar,
    dateVar,
    creatorsNamesVar,
    maybeDescriptionVar,
    keywordsVar,
    imagesVar
  ).map(_.name)

  override def query(criteria: Criteria): Option[String] =
    criteria.filters.whenRequesting(entityType) {
      fr"""SELECT $entityTypeVar
          |       $matchingScoreVar
          |       $nameVar
          |       $idsPathsVisibilitiesVar
          |       $sameAsVar
          |       $maybeDateCreatedVar
          |       $maybeDatePublishedVar
          |       $dateVar
          |       $creatorsNamesVar
          |       $maybeDescriptionVar
          |       $keywordsVar
          |       $imagesVar
          |WHERE {
          |  BIND ('dataset' AS $entityTypeVar)
          |
          |  # textQuery
          |  ${textQueryPart(criteria.filters.maybeQuery)}
          |
          |  # creators
          |  ${creatorsPart(criteria.filters.creators)}
          |
          |  # dates, keywords, description
          |  ${datesPart(criteria.filters.maybeSince, criteria.filters.maybeUntil)}
          |
          |  # images
          |  $imagesPart
          |
          |  # resolve project
          |  $resolveProject
          |
          |  # project namespaces
          |  ${namespacesPart(criteria.filters.namespaces)}
          |}
          |""".stripMargin.sparql
    }

  override def decoder[EE >: Entity.Dataset]: Decoder[EE] = DatasetsQuery.decoder

  def pathVisibility: Fragment =
//    |${
//      criteria.maybeOnAccessRightsAndVisibility("?projectId", "?visibility")
//    }
//    | BIND (CONCAT(STR(? identifier), STR(':'), STR(? projectPath), STR(':'), STR(? visibility)) AS ? idPathVisibility)

    Fragment.empty

  def imagesPart: Fragment =
    fr"""
        |  OPTIONAL {
        |    $dsId schema:image ?imageId .
        |    ?imageId schema:position ?imagePosition ;
        |             schema:contentUrl ?imageUrl .
        |    BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |  }
        |""".stripMargin

  def resolveProject: Fragment =
    fr"""
        |  Graph schema:Dataset {
        |    $dsId renku:datasetProjectLink ?linkId.
        |    ?linkId renku:project ?projectId
        |  }
        |""".stripMargin

  def datesPart(maybeSince: Option[Filters.Since], maybeUntil: Option[Filters.Until]): Fragment = {
    def dateCond(varName: VarName) = {
      val cond =
        List(
          maybeSince.map(s => fr"$varName >= ${s.value}"),
          maybeUntil.map(s => fr"$varName <= ${s.value}")
        ).flatten.intercalate(fr" && ")

      if (cond.isEmpty) Fragment.empty
      else fr"FILTER ($cond)"
    }

    fr"""
        |  Graph schema:Dataset {
        |    $dsId renku:slug $nameVar
        |    OPTIONAL {
        |      $dsId schema:dateCreated $maybeDateCreatedVar.
        |      BIND ($maybeDateCreatedVar AS $dateVar)
        |      ${dateCond(maybeDateCreatedVar)}
        |    }
        |    OPTIONAL {
        |      $dsId schema:datePublished $maybeDatePublishedVar
        |      BIND ($maybeDatePublishedVar AS $dateVar)
        |      ${dateCond(maybeDatePublishedVar)}
        |    }
        |    OPTIONAL {
        |      $dsId schema:keywords ?keyword
        |    }
        |    OPTIONAL {
        |      $dsId schema:description $maybeDescriptionVar
        |    }
        |  }
        |
        |""".stripMargin
  }

  def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"Values (?namespace) { ${ns.map(_.value)}  }"

    fr"""
        | GRAPH ?projectId {
        |  ?projectId renku:projectPath ?projectPath;
        |             renku:projectNamespace ?namespace
        |  $matchFrag
        |}
      """.stripMargin
  }

  def creatorsPart(creators: Set[persons.Name]): Fragment = {
    val matchFrag =
      if (creators.isEmpty) Fragment.empty
      else fr"Values (?creatorName) { ${creators.map(_.value)} }"

    fr"""
        |  Graph schema:Dataset {
        |    OPTIONAL {
        |      ?id_ schema:creator ?creatorId_.
        |      GRAPH schema:Person {
        |        ?creatorId_ schema:name ?creatorName
        |        $matchFrag
        |      }
        |    }
        |  }
        |
        |""".stripMargin
  }

  def textQueryPart(mq: Option[Filters.Query]): Fragment =
    mq match {
      case Some(q) =>
        val luceneQuery = LuceneQuery(q.value)
        fr"""
            |{
            |  SELECT $dsId (MAX(?score) AS $matchingScoreVar)
            |  WHERE {
            |    Graph schema:Dataset {
            |      ($dsId ?score) text:query (renku:slug schema:keywords schema:description schema:name $luceneQuery).
            |    }
            |  }
            |  group by $dsId
            |}
            |
            |""".stripMargin

      case None =>
        fr"""
            |  Bind (xsd:float(1.0) as $matchingScoreVar)
            |  Graph schema:Dataset {
            |    $dsId a renku:DiscoverableDataset.
            |  }
            """.stripMargin
    }
}
