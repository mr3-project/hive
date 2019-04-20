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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.HiveMR3ClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Simple implementation of <i>MR3SessionManager</i>
 *   - returns MR3Session when requested through <i>getSession</i> and keeps track of
 *       created sessions. Currently no limit on the number sessions.
 *   - MR3Session is reused if the userName in new conf and user name in session conf match.
 */
public class MR3SessionManagerImpl implements MR3SessionManager {
  private static final Log LOG = LogFactory.getLog(MR3SessionManagerImpl.class);

  private Set<MR3Session> createdSessions = Collections.synchronizedSet(new HashSet<MR3Session>());

  // guard with synchronize{}
  private boolean shareMr3Session = false;
  private MR3Session commonMr3Session = null;
  private boolean initializedClientFactory = false;

  private static MR3SessionManagerImpl instance;

  static {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          if (instance != null) {
            instance.shutdown();
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public static synchronized MR3SessionManagerImpl getInstance()
      throws HiveException {
    if (instance == null) {
      instance = new MR3SessionManagerImpl();
    }

    return instance;
  }

  private MR3SessionManagerImpl() {
  }

  // TODO: check that getSession() is called only after setup()

  // called directly from HiveServer2, in which case hiveConf comes from HiveSever2
  // called from nowhere else
  @Override
  public synchronized void setup(HiveConf hiveConf) throws HiveException {
    LOG.info("Setting up the session manager.");

    HiveMR3ClientFactory.initialize(hiveConf);
    initializedClientFactory = true;

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_MR3_SHARE_SESSION)) {
      // if MR3_SHARE_SESSION is enabled, the scratch directory should be created with permission 733 so that
      // each query can create its own MR3 scratch director, e.g.,
      // ..../<ugi.getShortUserName>/_mr3_scratch_dir-3/
      hiveConf.set(HiveConf.ConfVars.SCRATCHDIRPERMISSION.varname, "733");
      commonMr3Session = createSession(hiveConf, true);
      shareMr3Session = true;
    }
  }

  public synchronized boolean getShareMr3Session() {
    assert initializedClientFactory;  // after setup()

    return shareMr3Session;
  }

  @Override
  public synchronized MR3Session getSession(HiveConf hiveConf) throws HiveException {
    assert initializedClientFactory;  // after setup()

    if (shareMr3Session) {
      return commonMr3Session;
    } else {
      return createSession(hiveConf, false);
    }
  }

  // createSession() is called one at a time because it is in synchronized{}.
  private MR3Session createSession(HiveConf hiveConf, boolean shareSession) throws HiveException {
    String sessionUser;
    try {
      sessionUser = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new HiveException("No session user found", e);
    }
    MR3Session mr3Session = new MR3SessionImpl(shareSession, sessionUser);
    mr3Session.open(hiveConf);
    createdSessions.add(mr3Session);

    LOG.info("New MR3Session is created: " + mr3Session.getSessionId() + ", " + sessionUser);

    return mr3Session;
  }

  @Override
  public void returnSession(MR3Session mr3Session) throws HiveException {
  }

  @Override
  public void closeSession(MR3Session mr3Session) throws HiveException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Closing session (%s).", mr3Session.getSessionId()));
    }

    mr3Session.close();
    createdSessions.remove(mr3Session);
  }

  @Override
  public void shutdown() {
    LOG.info("Closing the session manager.");
    if (createdSessions != null) {
      synchronized (createdSessions) {
        Iterator<MR3Session> it = createdSessions.iterator();
        while (it.hasNext()) {
          MR3Session session = it.next();
          session.close();
        }
        createdSessions.clear();
      }
    }
  }
}
