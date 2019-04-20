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

import edu.postech.mr3.api.common.MR3Conf$;
import edu.postech.mr3.api.common.MR3ConfBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRefImpl;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import edu.postech.mr3.DAGAPI;
import edu.postech.mr3.api.client.DAGClient;
import edu.postech.mr3.api.client.MR3SessionClient;
import edu.postech.mr3.api.client.MR3SessionClient$;
import edu.postech.mr3.api.client.SessionStatus$;
import edu.postech.mr3.api.common.MR3Conf;
import edu.postech.mr3.api.common.MR3Exception;
import org.apache.tez.dag.api.TezConfiguration;
import scala.Option;

import java.util.Map;

public class HiveMR3ClientImpl implements HiveMR3Client {
  protected static final Log LOG = LogFactory.getLog(HiveMR3ClientImpl.class);

  private final MR3SessionClient mr3Client;
  private final HiveConf hiveConf;

  // initAmLocalResources[]: read-only 
  HiveMR3ClientImpl(
      String sessionId,
      final Credentials amCredentials,
      final Map<String, LocalResource> amLocalResources,
      final Credentials additionalSessionCredentials,
      final Map<String, LocalResource> additionalSessionLocalResources,
      HiveConf hiveConf) throws MR3Exception {
    this.hiveConf = hiveConf;

    MR3Conf mr3Conf = createMr3Conf(hiveConf);
    scala.collection.immutable.Map amLrs = MR3Utils.toScalaMap(amLocalResources);
    scala.collection.immutable.Map addtlSessionLrs = MR3Utils.toScalaMap(additionalSessionLocalResources);
    mr3Client = MR3SessionClient$.MODULE$.apply(
        sessionId, mr3Conf,
        Option.apply(amCredentials), amLrs,
        Option.apply(additionalSessionCredentials), addtlSessionLrs);
    mr3Client.start();
  }

  private MR3Conf createMr3Conf(HiveConf hiveConf) {
    JobConf jobConf = new JobConf(new TezConfiguration(hiveConf));
    // TODO: why not use the following?
    // DAGUtils dagUtils = DAGUtils.getInstance();
    // JobConf jobConf = dagUtils.createConfiguration(hiveConf);

    float maxJavaHeapFraction = HiveConf.getFloatVar(hiveConf,
        HiveConf.ConfVars.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION);

    // precedence: (hive-site.xml + command-line options) -> tez-site.xml/mapred-site.xml -> mr3-site.xml
    return new MR3ConfBuilder(true)
        .addResource(jobConf)
        .set(MR3Conf$.MODULE$.MR3_CONTAINER_MAX_JAVA_HEAP_FRACTION(), Float.toString(maxJavaHeapFraction))
        .setBoolean(MR3Conf$.MODULE$.MR3_AM_SESSION_MODE(), true).build();
  }

  @Override
  public MR3JobRef execute(
      final DAGAPI.DAGProto dagProto,
      final Credentials amCredentials,
      final Map<String, LocalResource> amLocalResources,
      final Map<String, BaseWork> workMap,
      final DAG dag,
      final Context ctx) throws Exception {

    scala.collection.immutable.Map addtlAmLrs = MR3Utils.toScalaMap(amLocalResources);
    DAGClient dagClient = mr3Client.submitDag(addtlAmLrs, Option.apply(amCredentials), dagProto);
    return new MR3JobRefImpl(hiveConf, dagClient, workMap, dag, ctx);
  }

  @Override
  public void close() {
    try {
      mr3Client.shutdownAppMaster();
      mr3Client.close();
    } catch (MR3Exception e) {
      LOG.warn("Failed to close MR3Client", e);
    }
  }

  @Override
  public MR3ClientState getClientState() throws Exception {
    SessionStatus$.Value sessionState = mr3Client.getSessionStatus();

    LOG.info("MR3ClientState: " + sessionState);

    if (sessionState == SessionStatus$.MODULE$.Initializing()) {
      return MR3ClientState.INITIALIZING;
    } else if (sessionState == SessionStatus$.MODULE$.Ready()
        || sessionState == SessionStatus$.MODULE$.Running()) {
      return MR3ClientState.READY;
    } else {
      return MR3ClientState.SHUTDOWN;
    }    
  }
}
