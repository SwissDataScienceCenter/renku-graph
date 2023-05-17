/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore.client.sparql

import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.tokenattributes.{CharTermAttribute, CharTermAttributeImpl}
import org.apache.lucene.util.AttributeFactory
import org.apache.lucene.util.AttributeFactory.StaticImplementationAttributeFactory

import java.io.StringReader

trait QueryTokenizer {
  def split(input: String): List[String]
}

object QueryTokenizer {

  def luceneStandard: QueryTokenizer = new QueryTokenizer {
    override def split(input: String): List[String] = {
      val tokenizer = new StandardTokenizer(
        new StaticImplementationAttributeFactory[CharTermAttributeImpl](
          AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY,
          classOf[CharTermAttributeImpl]
        ) {
          override def createInstance(): CharTermAttributeImpl = new CharTermAttributeImpl
        }
      )
      tokenizer.setReader(new StringReader(input))
      tokenizer.reset()
      val attr = tokenizer.addAttribute(classOf[CharTermAttribute])

      @annotation.tailrec
      def loop(result: List[String]): List[String] =
        if (tokenizer.incrementToken()) loop(attr.toString :: result)
        else result

      loop(Nil).reverse
    }
  }
}