/**
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

package org.apache.hadoop.hive.ql.exec.mr3.monitoring;

import com.google.common.base.Preconditions;
import edu.postech.mr3.api.client.DAGClient;
import edu.postech.mr3.api.client.DAGState$;
import edu.postech.mr3.api.client.DAGStatus;
import edu.postech.mr3.api.client.Progress;
import edu.postech.mr3.api.client.VertexStatus;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.common.log.InPlaceUpdate;
import org.apache.hadoop.hive.common.log.ProgressMonitor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr3.dag.DAG;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManager;
import org.apache.hadoop.hive.ql.exec.mr3.session.MR3SessionManagerImpl;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import scala.collection.JavaConversions$;

import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * MR3JobMonitor keeps track of an MR3 job while it's being executed. It will
 * print status to the console and retrieve final status of the job after
 * completion.
 */
public class MR3JobMonitor {

  private static final String CLASS_NAME = MR3JobMonitor.class.getName();
  private static final int CHECK_INTERVAL = 1000;
  private static final int MAX_RETRY_INTERVAL = 2500;
  private static final int PRINT_INTERVAL = 3000;

  private static final List<DAGClient> shutdownList;

  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private transient LogHelper console;

  interface UpdateFunction {
    void update(DAGStatus status, String report);
  }

  static {
    shutdownList = new LinkedList<DAGClient>();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        MR3JobMonitor.killRunningJobs();
        try {
          MR3SessionManager mr3SessionManager = MR3SessionManagerImpl.getInstance();
          System.err.println("Shutting down MR3 sessions.");
          mr3SessionManager.shutdown();
        } catch (Exception e) {
          // ignore
        }
      }
    });
  }

  public static void initShutdownHook() {
    Preconditions.checkNotNull(shutdownList,
        "Shutdown hook was not properly initialized");
  }

  private final Map<String, BaseWork> workMap;
  private final DAGClient dagClient;
  private final HiveConf hiveConf;
  private final DAG dag;
  private final Context context;
  private final UpdateFunction updateFunction;
  /**
   * Have to use the same instance to render else the number lines printed earlier is lost and the
   * screen will print the table again and again.
   */
  private final InPlaceUpdate inPlaceUpdate;

  private long executionStartTime = 0;
  private DAGStatus dagStatus = null;
  private long lastPrintTime;
  private StringWriter diagnostics = new StringWriter();

  public MR3JobMonitor(Map<String, BaseWork> workMap, final DAGClient dagClient, HiveConf conf, DAG dag,
      Context ctx) {
    this.workMap = workMap;
    this.dagClient = dagClient;
    this.hiveConf = conf;
    this.dag = dag;
    this.context = ctx;
    console = SessionState.getConsole();
    inPlaceUpdate = new InPlaceUpdate(LogHelper.getInfoStream());
    updateFunction = updateFunction();
  }

  private UpdateFunction updateFunction() {
    UpdateFunction logToFileFunction = new UpdateFunction() {
      @Override
      public void update(DAGStatus status, String report) {
        SessionState.get().updateProgressMonitor(progressMonitor(status));
        console.printInfo(report);
      }
    };
    UpdateFunction inPlaceUpdateFunction = new UpdateFunction() {
      @Override
      public void update(DAGStatus status, String report) {
        inPlaceUpdate.render(progressMonitor(status));
        console.logInfo(report);
      }
    };
    return InPlaceUpdate.canRenderInPlace(hiveConf)
        && !SessionState.getConsole().getIsSilent()
        && !SessionState.get().isHiveServerQuery()
        ? inPlaceUpdateFunction : logToFileFunction;
  }

  private boolean isProfilingEnabled() {
    return HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.MR3_EXEC_SUMMARY) ||
      Utilities.isPerfOrAboveLogging(hiveConf);
  }

  /**
   * monitorExecution handles status printing, failures during execution and final status retrieval.
   *
   * @return int 0 - success, 1 - killed, 2 - failed
   */
  public int monitorExecution() {
    boolean done = false;
    boolean success = false;
    int failedCounter = 0;
    int rc = 0;

    long monitorStartTime = System.currentTimeMillis();
    synchronized (shutdownList) {
      shutdownList.add(dagClient);
    }
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_RUN_DAG);
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_SUBMIT_TO_RUNNING);
    DAGState$.Value lastState = null;
    String lastReport = null;
    boolean running = false;

    while (true) {
      try {
        if (context != null) {
          context.checkHeartbeaterLockException();
        }

        dagStatus = dagClient.getDagStatusWait(false, CHECK_INTERVAL).get();
        DAGState$.Value state = dagStatus.state();

        if (state != lastState || state == DAGState$.MODULE$.Running()) {
          lastState = state;

          if (state == DAGState$.MODULE$.New()) {
            console.printInfo("Status: New");
            this.executionStartTime = System.currentTimeMillis();
          } else if (state == DAGState$.MODULE$.Running()) {
            if (!running) {
              perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_SUBMIT_TO_RUNNING);
              console.printInfo("Status: Running\n");
              this.executionStartTime = System.currentTimeMillis();
              running = true;
            }
            lastReport = updateStatus(dagStatus, lastReport);
          } else if (state == DAGState$.MODULE$.Succeeded()) {
            if (!running) {
              this.executionStartTime = monitorStartTime;
            }
            lastReport = updateStatus(dagStatus, lastReport);
            success = true;
            running = false;
            done = true;
          } else if (state == DAGState$.MODULE$.Killed()) {
            if (!running) {
              this.executionStartTime = monitorStartTime;
            }
            lastReport = updateStatus(dagStatus, lastReport);
            console.printInfo("Status: Killed");
            running = false;
            done = true;
            rc = 1;
          } else if (state == DAGState$.MODULE$.Failed()) {
            if (!running) {
              this.executionStartTime = monitorStartTime;
            }
            lastReport = updateStatus(dagStatus, lastReport);
            console.printError("Status: Failed");
            running = false;
            done = true;
            rc = 2;
          }
        }
      } catch (Exception e) {
        console.printInfo("Exception: " + e.getMessage());
        boolean isInterrupted = hasInterruptedException(e);
        if (isInterrupted || (++failedCounter % MAX_RETRY_INTERVAL / CHECK_INTERVAL == 0)) {
          console.printInfo("Killing DAG...");
          dagClient.tryKillDag();
          console.printError("Execution has failed. stack trace: " + ExceptionUtils.getStackTrace(e));
          rc = 1;
          done = true;
        } else {
          console.printInfo("Retrying...");
        }
      } finally {
        if (done) {
          if (rc == 0 && dagStatus != null) {
            console.printInfo("Status: Succeeded");
            for (String diag : JavaConversions$.MODULE$.asJavaCollection(dagStatus.diagnostics())) {
              console.printInfo(diag);
            }
          } else if (rc != 0 && dagStatus != null) {
            for (String diag : JavaConversions$.MODULE$.asJavaCollection(dagStatus.diagnostics())) {
              console.printError(diag);
              diagnostics.append(diag);
            }
          }
          synchronized (shutdownList) {
            shutdownList.remove(dagClient);
          }
          break;
        }
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_RUN_DAG);
    printSummary(success, dagStatus);
    return rc;
  }

  private void printSummary(boolean success, DAGStatus status) {
    if (isProfilingEnabled() && success && status != null) {

      double duration = (System.currentTimeMillis() - this.executionStartTime) / 1000.0;
      console.printInfo("Status: DAG finished successfully in " + String.format("%.2f seconds", duration));
      console.printInfo("");

      Map<String, VertexStatus> vertexStatusMap =
          JavaConversions$.MODULE$.mapAsJavaMap(status.vertexStatusMap());

      new QueryExecutionBreakdownSummary(perfLogger).print(console);
      new DAGSummary(vertexStatusMap, status, hiveConf, dag, perfLogger).print(console);

      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.LLAP_IO_ENABLED, false)) {
        new LLAPioSummary(vertexStatusMap).print(console);
        new FSCountersSummary(vertexStatusMap).print(console);
      }
      console.printInfo("");
    }
  }

  private static boolean hasInterruptedException(Throwable e) {
    // Hadoop IPC wraps InterruptedException. GRRR.
    while (e != null) {
      if (e instanceof InterruptedException || e instanceof InterruptedIOException) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  /**
   * killRunningJobs tries to terminate execution of all
   * currently running MR3 queries. No guarantees, best effort only.
   */
  public static void killRunningJobs() {
    synchronized (shutdownList) {
      for (DAGClient c : shutdownList) {
        try {
          System.err.println("Trying to shutdown DAG");
          c.tryKillDag();
        } catch (Exception e) {
          // ignore
        }
      }
    }
  }

  static long getCounterValueByGroupName(TezCounters vertexCounters,
      String groupNamePattern,
      String counterName) {
    TezCounter tezCounter = vertexCounters.getGroup(groupNamePattern).findCounter(counterName);
    return (tezCounter == null) ? 0 : tezCounter.getValue();
  }

  private String updateStatus(DAGStatus status, String lastReport) {
    String report = getReport(status);
    if (!report.equals(lastReport) || System.currentTimeMillis() >= lastPrintTime + PRINT_INTERVAL) {
      updateFunction.update(status, report);
      lastPrintTime = System.currentTimeMillis();
    }
    return report;
  }

  private String getReport(DAGStatus status) {
    StringBuilder reportBuffer = new StringBuilder();

    Map<String, VertexStatus> vertexStatusMap =
        JavaConversions$.MODULE$.mapAsJavaMap(status.vertexStatusMap());
    SortedSet<String> keys = new TreeSet<String>(vertexStatusMap.keySet());
    for (String s : keys) {
      Progress progress = vertexStatusMap.get(s).progress();
      final int complete = progress.numSucceededTasks();
      final int total = progress.numTasks();
      final int running = progress.numScheduledTasks();
      final int failed = progress.numFailedTaskAttempts();
      if (total <= 0) {
        reportBuffer.append(String.format("%s: -/-\t", s));
      } else {
        if (complete == total) {
          /*
           * We may have missed the start of the vertex due to the 3 seconds interval
           */
          if (!perfLogger.startTimeHasMethod(PerfLogger.MR3_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_RUN_VERTEX + s);
          }

          if (!perfLogger.endTimeHasMethod(PerfLogger.MR3_RUN_VERTEX + s)) {
            perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.MR3_RUN_VERTEX + s);
          }
        }
        if (complete < total && (complete > 0 || running > 0 || failed > 0)) {

          if (!perfLogger.startTimeHasMethod(PerfLogger.MR3_RUN_VERTEX + s)) {
            perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.MR3_RUN_VERTEX + s);
          }

          /* vertex is started, but not complete */
          if (failed > 0) {
            reportBuffer.append(String.format("%s: %d(+%d,-%d)/%d\t", s, complete, running, failed, total));
          } else {
            reportBuffer.append(String.format("%s: %d(+%d)/%d\t", s, complete, running, total));
          }
        } else {
          /* vertex is waiting for input/slots or complete */
          if (failed > 0) {
            /* tasks finished but some failed */
            reportBuffer.append(String.format("%s: %d(-%d)/%d\t", s, complete, failed, total));
          } else {
            reportBuffer.append(String.format("%s: %d/%d\t", s, complete, total));
          }
        }
      }
    }

    return reportBuffer.toString();
  }

  public String getDiagnostics() {
    return diagnostics.toString();
  }

  public TezCounters getDagCounters() {
    try {
      return dagStatus.counters().get();
    } catch (Exception e) {
    }
    return null;
  }

  private ProgressMonitor progressMonitor(DAGStatus status) {
    return new MR3ProgressMonitor(status, workMap, console, executionStartTime);
  }
}
