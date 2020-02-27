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

package com.metamx.health.hq.db

import com.metamx.common.scala.Predef._
import com.metamx.common.scala.db.{DB, DBConfig}

class PostgreSQLDB(config: DBConfig) extends DB(config) {
  override def createIsTransient: Throwable => Boolean = {
    var timeoutLimit = 3

    // TODO: rtfm and confirm i am actually catching all errors that could be considered transient, ...and not any that are not...
    def isTransient(e: Throwable): Boolean =
      e match {

        // If our query repeatedly fails to finish, then we should probably stop doing it
        case e: org.postgresql.util.PSQLException =>
          val code = e.getErrorCode
          code match {
            case
              0x08000 |
              0x08001 |
              0x08003 |
              0x08004 |
              0x08006
            =>
              (timeoutLimit > 1) andThen {
                timeoutLimit -= 1
              }
            case _ => false
          }


        // Anything marked "transient"
        case e: java.sql.SQLTransientException                                       => true

        // IO errors from jdbc look like this [are we responsible for force-closing the connection in this case?]
        case e: java.sql.SQLRecoverableException                                     => true

        // Specific errors from jdbi with no useful supertype
        case e: org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException       => true
        //case e: org.skife.jdbi.v2.exceptions.UnableToCloseResourceException        => true // TODO Include this one?

        // MySQL ER_QUERY_INTERRUPTED "Query execution was interrupted" [ETL-153]
        case e: java.sql.SQLException                     if e.getErrorCode == 1317  => true

        // MySQL ER_LOCK_WAIT_TIMEOUT "Lock wait timeout exceeded; try restarting transaction"
        case e: java.sql.SQLException                     if e.getErrorCode == 1205  => true

        // Unwrap nested exceptions from jdbc and jdbi
        case e: java.sql.SQLException                     if isTransient(e.getCause) => true
        case e: org.skife.jdbi.v2.exceptions.DBIException if isTransient(e.getCause) => true

        // Nothing else, including nulls
        case _                                                                       => false

      }

    isTransient _

  }

  override def createTable(table: String, decls: Seq[String]) {
    execute("create table %s (%s)" format (table, decls mkString ", "))
  }
}
