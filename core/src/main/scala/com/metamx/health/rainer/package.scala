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

package com.metamx.health

import com.metamx.rainer.Commit

package object rainer
{
  object RainerUtils
  {
    case class ConfigData[A](config: A, str: String)

    def configByKey[A](
      key: String,
      configs: Map[Commit.Key, Commit[A]]
    ): Option[ConfigData[A]] = {
      configs.get(key) match {
        case Some(commit) =>
          commit.value match {
            case Some(Right(config)) =>
              Some(ConfigData(config, commit.payload.map(x => new String(x)).getOrElse("")))

            case Some(Left(e)) =>
              throw new IllegalStateException("Unable to deserialize payload for key '%s'".format(key), e)

            case None =>
              throw new IllegalStateException("Payload is empty for key '%s'".format(key))
          }

        case None =>
          None
      }
    }
  }
}
