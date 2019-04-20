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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Defines interface for managing multiple MR3Sessions in Hive when multiple users
 * are executing queries simultaneously on MR3 execution engine.
 */
public interface MR3SessionManager {
  /**
   * Initialize based on given configuration.
   *
   * @param hiveConf
   */
  void setup(HiveConf hiveConf) throws HiveException;

  boolean getShareMr3Session();

  /**
   *
   * @param conf
   * @return MR3Session
   */
  MR3Session getSession(HiveConf conf) throws HiveException;

  /**
   * Return the given <i>mr3Session</i> to pool. This is used when the client
   * still holds references to session and may want to reuse it in future.
   * When client wants to reuse the session, it should pass it to the <i>getSession</i> method.
   */
  void returnSession(MR3Session mr3Session) throws HiveException;

  /**
   * Close the given session and return it to pool. This is used when the client
   * no longer needs a MR3Session.
   */
  void closeSession(MR3Session mr3Session) throws HiveException;

  /**
   * Shutdown the session manager. Also closing up MR3Sessions in pool.
   */
  void shutdown();
}
