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

package org.apache.hadoop.hive.ql.exec.mr3.session;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr3.DAGUtils;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3Client;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3Client.MR3ClientState;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3ClientFactory;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.status.MR3JobRef;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.ConverterUtils;
import edu.postech.mr3.DAGAPI;
import edu.postech.mr3.api.common.MR3Conf;
import edu.postech.mr3.api.common.MR3Conf$;
import edu.postech.mr3.api.common.MR3ConfBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MR3SessionImpl implements MR3Session {

  private static final String CLASS_NAME = MR3SessionImpl.class.getName();
  private static final Log LOG = LogFactory.getLog(MR3Session.class);
  private static final String MR3_DIR = "_mr3_session_dir";
  private static final String MR3_AM_STAGING_DIR = "staging";

  private final boolean shareMr3Session;
  private final String sessionId;
  private final String sessionUser;

  // set in open() and close()
  // read in submit() via updateAmCredentials()
  private HiveConf sessionConf;
  // read in submit()
  private HiveMR3Client hiveMr3Client;

  // set in open() and close()
  // read from MR3Task thread via getSessionScratchDir()
  private Path sessionScratchDir;

  // updated in open() and submit()
  // via updateAmLocalResources()
  private Map<String, LocalResource> amLocalResources = new HashMap<String, LocalResource>();
  // via updateAmCredentials()
  private Credentials amCredentials;

  // private List<LocalResource> amDagCommonLocalResources = new ArrayList<LocalResource>();

  DAGUtils dagUtils = DAGUtils.getInstance();

  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }

  public MR3SessionImpl(boolean shareMr3Session, String sessionUser) {
    this.shareMr3Session = shareMr3Session;
    this.sessionId = makeSessionId();
    this.sessionUser = sessionUser;
  }

  // if shareMr3Session == false, close() can be called only from the owner MR3Task/SessionState.
  // if shareMr3Session == true, close() is called only from MR3SessionManager.shutdown() at the end.

  @Override
  public synchronized void open(HiveConf conf) throws HiveException {
    this.sessionConf = conf;
    try {
      sessionScratchDir = createSessionScratchDir(sessionId);
      setAmStagingDir(sessionScratchDir);

      // 1. read hiveJarLocalResources and confLocalResources

      // getSessionInitJars() returns hive-exec.jar + HIVEAUXJARS
      List<LocalResource> hiveJarLocalResources =
        dagUtils.localizeTempFiles(sessionScratchDir.toString(), conf, dagUtils.getSessionInitJars(conf));
      Map<String, LocalResource> additionalSessionLocalResources =
          dagUtils.convertLocalResourceListToMap(hiveJarLocalResources);

      Credentials additionalSessionCredentials = new Credentials();
      Set<Path> allPaths = new HashSet<Path>();
      for (LocalResource lr: additionalSessionLocalResources.values()) {
        allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
      }
      dagUtils.addPathsToCredentials(additionalSessionCredentials, allPaths, conf);

      // 2. read confLocalResources

      // confLocalResource = specific to this MR3Session obtained from sessionConf
      // localizeTempFilesFromConf() updates sessionConf by calling HiveConf.setVar(HIVEADDEDFILES/JARS/ARCHIVES)
      List<LocalResource> confLocalResources =
        dagUtils.localizeTempFilesFromConf(sessionScratchDir.toString(), conf);

      // We do not add confLocalResources to additionalSessionLocalResources because
      // dagUtils.localizeTempFilesFromConf() will be called each time a new DAG is submitted.

      // 3. set initAmLocalResources

      List<LocalResource> initAmLocalResources = new ArrayList<LocalResource>();
      initAmLocalResources.addAll(confLocalResources);
      Map<String, LocalResource> initAmLocalResourcesMap =
        dagUtils.convertLocalResourceListToMap(initAmLocalResources);

      // 4. update amLocalResource and create HiveMR3Client

      updateAmLocalResources(initAmLocalResourcesMap);
      updateAmCredentials(initAmLocalResourcesMap);

      LOG.info(
          "Opening a new MR3 Session (id: " + sessionId + ", scratch dir: " + sessionScratchDir + ")");
      hiveMr3Client = HiveMR3ClientFactory.createHiveMr3Client(
          sessionId,
          amCredentials, amLocalResources,
          additionalSessionCredentials, additionalSessionLocalResources,
          conf);

      // 4. wait until ready

      LOG.info("Waiting until MR3Client starts and transitions to Ready");
      waitUntilMr3ClientReady();
    } catch (Exception e) {
      LOG.error("Failed to open MR3 Session", e);
      close();
      throw new HiveException("Failed to create or start MR3Client", e);
    }
  }

  private void setAmStagingDir(Path sessionScratchDir) {
    Path amStagingDir = new Path(sessionScratchDir, MR3_AM_STAGING_DIR);
    sessionConf.set(MR3Conf$.MODULE$.MR3_AM_STAGING_DIR(), amStagingDir.toUri().toString());
    // amStagingDir is created by MR3 in ApplicationSubmissionContextBuilder.build()
  }

  /**
   * createSessionScratchDir creates a temporary directory in the scratchDir folder to
   * be used with mr3. Assumes scratchDir exists.
   */
  private Path createSessionScratchDir(String sessionId) throws IOException {
    //TODO: ensure this works in local mode, and creates dir on local FS
    // MR3 needs its own scratch dir (per session)
    Path mr3SessionScratchDir = new Path(SessionState.get().getHdfsScratchDirURIString(), MR3_DIR);
    mr3SessionScratchDir = new Path(mr3SessionScratchDir, sessionId);
    FileSystem fs = mr3SessionScratchDir.getFileSystem(sessionConf);
    Utilities.createDirsWithPermission(
        sessionConf, mr3SessionScratchDir, new FsPermission(SessionState.SESSION_SCRATCH_DIR_PERMISSION), true);
    // Make sure the path is normalized.
    FileStatus dirStatus = DAGUtils.validateTargetDir(mr3SessionScratchDir, sessionConf);
    assert dirStatus != null;

    mr3SessionScratchDir = dirStatus.getPath();
    LOG.info("Created MR3 Session Scratch Dir: " + mr3SessionScratchDir);

    // don't keep the directory around on non-clean exit
    fs.deleteOnExit(mr3SessionScratchDir);

    return mr3SessionScratchDir;
  }

  @Override
  public synchronized void close() {
    if (hiveMr3Client != null) {
      hiveMr3Client.close();
    }
    hiveMr3Client = null;

    amLocalResources.clear();
    amCredentials = null;

    if (sessionScratchDir != null) {
      cleanupSessionScratchDir();
    }

    sessionConf = null;
  }

  private void cleanupSessionScratchDir() {
    dagUtils.cleanMr3Dir(sessionScratchDir, sessionConf);
    sessionScratchDir = null;
  }

  public synchronized Path getSessionScratchDir() {
    return sessionScratchDir;
  }

  @Override
  public MR3JobRef submit(
      DAG dag,
      Map<String, LocalResource> newAmLocalResources,
      Configuration mr3TaskConf,
      Map<String, BaseWork> workMap,
      Context ctx, PerfLogger perfLogger) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);

    // This code block is not really necessary and we could just read hiveMr3Client directly
    // because before calling submit(), MR3Task always calls getSessionScratchDir(), at which point
    // synchronization takes place with the thread that previously initialized hiveMr3Client.
    // TODO: use hiveMr3Client and remove currentHiveMr3Client
    HiveMR3Client currentHiveMr3Client;
    synchronized (this) {
      currentHiveMr3Client = hiveMr3Client;
    }
    Preconditions.checkState(isOpen(currentHiveMr3Client), "Session is not open. Can't submit jobs.");

    Map<String, LocalResource> addtlAmLocalResources;
    Credentials addtlAmCredentials;
    synchronized (this) {
      // add newAmLocalResources to this.amLocalResources
      addtlAmLocalResources = updateAmLocalResources(newAmLocalResources);
      addtlAmCredentials = updateAmCredentials(newAmLocalResources);
    }

    String dagUser = UserGroupInformation.getCurrentUser().getShortUserName();
    MR3Conf dagConf = createDagConf(mr3TaskConf, dagUser);

    // sessionConf is not passed to MR3; only dagConf is passed to MR3 as a component of DAGProto.dagConf.
    DAGAPI.DAGProto dagProto = dag.createDagProto(mr3TaskConf, dagConf);

    MR3JobRef mr3JobRef = currentHiveMr3Client.execute(
        dagProto, addtlAmCredentials, addtlAmLocalResources, workMap, dag, ctx);

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_SUBMIT_DAG);
    return mr3JobRef; 
  }

  private boolean isOpen(HiveMR3Client currentHiveMr3Client) throws Exception {
    return
        (currentHiveMr3Client != null) &&
        (currentHiveMr3Client.getClientState() != MR3ClientState.SHUTDOWN);
  }

  // TODO: MR3Conf from createDagConf() is the only MR3Conf passed to MR3 as part of submitting a DAG.
  // Currently we set only MR3Conf.MR3_CONTAINER_STOP_CROSS_DAG_REUSE.

  // sessionConf == Configuration specific to the DAG being submitted
  private MR3Conf createDagConf(Configuration mr3TaskConf, String dagUser) {
    boolean confStopCrossDagReuse = HiveConf.getBoolVar(mr3TaskConf,
        HiveConf.ConfVars.MR3_CONTAINER_STOP_CROSS_DAG_REUSE);
    if (shareMr3Session) {
      // TODO: if HIVE_SERVER2_ENABLE_DOAS is false, sessionUser.equals(dagUser) is always true
      boolean stopCrossDagReuse = sessionUser.equals(dagUser) && confStopCrossDagReuse;
      // do not add sessionConf because Configuration for MR3Session should be reused.
      return new MR3ConfBuilder(false)
          .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_STOP_CROSS_DAG_REUSE(), stopCrossDagReuse)
          .build();
    } else {
      // add sessionConf because this session is for the DAG being submitted.
      return new MR3ConfBuilder(false)
          .addResource(mr3TaskConf)
          .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_STOP_CROSS_DAG_REUSE(), confStopCrossDagReuse)
          .build();
    }
  }

  @Override
  public String getSessionId() {
    return sessionId;
  }

  /**
   * @param localResources
   * @return Map of newly added AM LocalResources
   */
  private Map<String, LocalResource> updateAmLocalResources(
      Map<String, LocalResource> localResources ) {
    Map<String, LocalResource> addtlLocalResources = new HashMap<String, LocalResource>();

    for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {
      if (!amLocalResources.containsKey(entry.getKey())) {
        amLocalResources.put(entry.getKey(), entry.getValue());
        addtlLocalResources.put(entry.getKey(), entry.getValue());
      }
    }

    return addtlLocalResources;
  }

  /**
   * @param localResources to be added to Credentials
   * @return returns Credentials for newly added LocalResources only
   */
  private Credentials updateAmCredentials(
      Map<String, LocalResource> localResources) throws Exception {
    if (amCredentials == null) {
      amCredentials = new Credentials();
    }

    Set<Path> allPaths = new HashSet<Path>();
    for (LocalResource lr: localResources.values()) {
      allPaths.add(ConverterUtils.getPathFromYarnURL(lr.getResource()));
    }

    Credentials addtlAmCredentials = new Credentials();
    dagUtils.addPathsToCredentials(addtlAmCredentials, allPaths, sessionConf);

    // hadoop-1 version of Credentials doesn't have method mergeAll()
    // See Jira HIVE-6915 and HIVE-8782
    // TODO: use ShimLoader.getHadoopShims().mergeCredentials(jobConf, addtlJobConf)
    amCredentials.addAll(addtlAmCredentials);

    return addtlAmCredentials;
  }

  private void waitUntilMr3ClientReady() throws Exception {
    long timeoutMs = sessionConf.getTimeVar(
        HiveConf.ConfVars.MR3_CLIENT_CONNECT_TIMEOUT, TimeUnit.MILLISECONDS);
    long endTimeoutTimeMs = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < endTimeoutTimeMs) {
      try {
        if (isMr3ClientReady()) {
          return;
        }
      } catch (Exception ex) {
        LOG.info("Exception while waiting for MR3Client state: " + ex.getClass().getSimpleName());
      }
      Thread.sleep(1000);
    }
    throw new Exception("MR3Client failed to start or transition to Ready");
  }

  private boolean isMr3ClientReady() throws Exception {
    assert(hiveMr3Client != null);
    MR3ClientState state = hiveMr3Client.getClientState();
    LOG.info("Current MR3Client state = " + state.toString());
    return state == MR3ClientState.READY;
  }
}
