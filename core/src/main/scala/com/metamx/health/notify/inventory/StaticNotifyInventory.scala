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

package com.metamx.health.notify.inventory

import com.metamx.common.scala.Logging
import com.metamx.health.core.config.ServiceConfig
import com.metamx.health.notify.{MailBuffer, Mailer, PagerDuty}
import com.metamx.health.util.AssertiveCloseable

class StaticNotifyInventory(config: NotifyInventoryConfig, serviceConfig: ServiceConfig)
  extends NotifyInventory with AssertiveCloseable with Logging
{
  log.info("Initializing inventory")
  private val mailer = new Mailer(config.mailerConfig)
  private val mailBuffer = new MailBuffer(mailer, config.mailBufferConfig)
  private val pagerDuty = new PagerDuty(mailBuffer, config.pagerDutyConfig, serviceConfig)
  log.info("Inventory initialized")

  override def withMailer[A](f: (Mailer) => A): A = {
    assertiveExecute(f(mailer))
  }

  override def withMailBuffer[A](f: (MailBuffer) => A): A = {
    assertiveExecute(f(mailBuffer))
  }

  override def withPagerDuty[A](f: (PagerDuty) => A): A = {
    assertiveExecute(f(pagerDuty))
  }

  protected override def assertiveClose() {
    log.info("Closing inventory")
    pagerDuty.close()
    mailBuffer.close()
    log.info("Inventory closed")
  }

  override def toString = "StaticNotifyInventory"
}
