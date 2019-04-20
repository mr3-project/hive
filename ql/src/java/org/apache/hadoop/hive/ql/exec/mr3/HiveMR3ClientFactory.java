/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.mr3;

import edu.postech.mr3.api.common.MR3Exception;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.util.Map;

public class HiveMR3ClientFactory {
  protected static final transient Log LOG = LogFactory.getLog(HiveMR3ClientFactory.class);

  public static void initialize(HiveConf hiveConf) {
    LOG.info("Initializing HiveMR3ClientFactory.");
  }

  // amLocalResources[]: read-only
  public static HiveMR3Client createHiveMr3Client(
      String sessionId,
      Credentials amCredentials,
      Map<String, LocalResource> amLocalResources,
      Credentials additionalSessionCredentials,
      Map<String, LocalResource> additionalSessionLocalResources,
      HiveConf hiveConf)
    throws MR3Exception {

    return new HiveMR3ClientImpl(
        sessionId,
        amCredentials, amLocalResources,
        additionalSessionCredentials, additionalSessionLocalResources,
        hiveConf);
  }
}
