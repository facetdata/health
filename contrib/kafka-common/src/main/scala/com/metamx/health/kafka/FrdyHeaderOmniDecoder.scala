/*
 * Licensed to Facet Data, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Facet Data, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.metamx.health.kafka

import com.fasterxml.jackson.dataformat.smile.{SmileFactory, SmileGenerator, SmileParser}
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.common.scala.untyped._
import com.metamx.frdy.codec._
import net.jpountz.lz4.LZ4Factory

class FrdyHeaderOmniDecoder(decoders: FrdyDecoder[_]*)
{
  def detect(bytes: Array[Byte], offset: Int, length: Int): Boolean = {
    decoders.exists(_.detect(bytes, offset, length))
  }

  def decodeHeader(bytes: Array[Byte]): Dict = {
    val decoder = decoders.find(_.detect(bytes)) getOrElse {
      throw new IllegalStateException("Can't decode these bytes!")
    }
    decoder.decode(bytes).header
  }
}

object FrdyHeaderOmniDecoder
{
  val decoder = {
    val smileMapper = Jackson.newObjectMapper(new SmileFactory)
    val smileFactoryForPlainDecoder = new SmileFactory().withEffect { sf =>
      sf.disable(SmileGenerator.Feature.WRITE_HEADER)
      sf.disable(SmileParser.Feature.REQUIRE_HEADER)
    }
    val jsonMapper = Jackson.newObjectMapper()
    val lz4Decompressor = LZ4Factory.fastestJavaInstance().decompressor()
    new FrdyHeaderOmniDecoder(
      new SmileLz4Decoder[Dict](smileMapper, lz4Decompressor),
      new JsonLz4Decoder[Dict](jsonMapper, lz4Decompressor),
      new PlainJsonDecoder[Dict](jsonMapper),
      new PlainBytesDecoder(Jackson.newObjectMapper(smileFactoryForPlainDecoder))
    )
  }
}
