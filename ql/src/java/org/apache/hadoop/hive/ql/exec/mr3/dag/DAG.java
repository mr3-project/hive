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

package org.apache.hadoop.hive.ql.exec.mr3.dag;

import com.google.protobuf.ByteString;
import edu.postech.mr3.api.common.MR3Conf$;
import edu.postech.mr3.api.common.MR3ConfBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr3.DAGUtils;
import org.apache.hadoop.hive.ql.exec.mr3.MR3Utils;
import org.apache.hadoop.hive.ql.exec.mr3.llap.LLAPDaemonProcessor;
import org.apache.hadoop.hive.ql.exec.mr3.llap.LLAPDaemonVertexManagerPlugin;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import edu.postech.mr3.DAGAPI;
import edu.postech.mr3.common.CommonUtils;
import edu.postech.mr3.api.common.MR3Conf;
import edu.postech.mr3.api.security.DAGAccessControls;
import edu.postech.mr3.api.util.ProtoConverters;
import edu.postech.mr3.api.common.Utils$;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DAG {

  final private String name;
  final private String dagInfo;
  final private Credentials dagCredentials;
  final private DAGAccessControls dagAccessControls;

  final private Collection<LocalResource> localResources = new HashSet<LocalResource>();
  final private Map<String, Vertex> vertices = new HashMap<String, Vertex>();
  final private List<VertexGroup> vertexGroups = new ArrayList<VertexGroup>();
  final private List<Edge> edges = new ArrayList<Edge>();

  public static enum ContainerGroupScheme { ALL_IN_ONE, PER_MAP_REDUCE, PER_VERTEX }

  public static final String ALL_IN_ONE_CONTAINER_GROUP_NAME = "All-In-One";
  public static final String PER_MAP_CONTAINER_GROUP_NAME = "Per-Map";
  public static final String PER_REDUCE_CONTAINER_GROUP_NAME = "Per-Reduce";

  public static final int allInOneContainerGroupPriority = 0;
  public static final int perMapContainerGroupPriority = 0;
  public static final int perReduceContainerGroupPriority = perMapContainerGroupPriority + 3;

  public static final int defaultLlapDaemonTaskMemoryMb = 1024;
  public static final int defaultLlapDaemonTaskVcores = 1;

  private DAG(
      String name,
      String dagInfo,
      @Nullable Credentials dagCredentials,
      DAGAccessControls dagAccessControls) {
    this.name = name;
    this.dagInfo = dagInfo;
    this.dagCredentials = dagCredentials != null ? dagCredentials : new Credentials();
    this.dagAccessControls = dagAccessControls;
  }

  public static DAG create(
      String name,
      String dagInfo,
      Credentials dagCredentials,
      DAGAccessControls dagAccessControls) {
    return new DAG(name, dagInfo, dagCredentials, dagAccessControls);
  }

  /**
   * adds Paths to Dag Credentials
   * @param paths
   * @throws IOException
   */
  public void addPathsToCredentials(
      DAGUtils dagUtils, Set<Path> paths, Configuration conf) throws IOException {
    dagUtils.addPathsToCredentials(dagCredentials, paths, conf);
  }

  public void addLocalResources(Collection<LocalResource> localResources) {
    this.localResources.addAll(localResources);
  }

  public void addVertex(Vertex vertex) {
    assert !vertices.containsKey(vertex.getName());
    vertices.put(vertex.getName(), vertex);
  }

  /**
   * @return unmodifiableMap of Vertices
   */
  public Map<String, Vertex> getVertices() {
    return Collections.unmodifiableMap(vertices);
  }

  public void addVertexGroup(VertexGroup vertexGroup) {
    vertexGroups.add(vertexGroup);

    for (Vertex member: vertexGroup.getMembers()) {
      for (GroupInputEdge gedge: vertexGroup.getEdges()) {
        Vertex destVertex = gedge.getDestVertex(); 
        EdgeProperty edgeProperty = gedge.getEdgeProperty(); 
        Edge edge = new Edge(member, destVertex, edgeProperty); 
        addEdge(edge);
      }
    }
  }

  public void addEdge(Edge edge) {
    Vertex srcVertex = edge.getSrcVertex();
    Vertex destVertex = edge.getDestVertex();
    assert vertices.containsKey(srcVertex.getName());
    assert vertices.containsKey(destVertex.getName());

    srcVertex.addOutputEdge(edge);
    destVertex.addInputEdge(edge);
    edges.add(edge);
  }

  public DAGAPI.DAGProto createDagProto(Configuration conf, MR3Conf dagConf) throws IOException {
    ContainerGroupScheme scheme = getContainerGroupScheme(conf);

    List<DAGAPI.VertexProto> vertexProtos = createVertexProtos(scheme);

    List<DAGAPI.EdgeProto> edgeProtos = new ArrayList<DAGAPI.EdgeProto>();
    for (Edge edge: edges) {
      edgeProtos.add(edge.createEdgeProto());
    }

    List<DAGAPI.VertexGroupProto> vertexGroupProtos = new ArrayList<DAGAPI.VertexGroupProto>();
    for (VertexGroup vertexGrp: vertexGroups) {
      vertexGroupProtos.add(vertexGrp.createVertexGroupProto());
    }

    List<DAGAPI.LocalResourceProto> lrProtos = new ArrayList<DAGAPI.LocalResourceProto>();
    DAGUtils dagUtils = DAGUtils.getInstance();
    for (LocalResource lr: localResources) {
      lrProtos.add(ProtoConverters.convertToLocalResourceProto(dagUtils.getBaseName(lr), lr));
    }

    boolean useLlapIo = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_IO_ENABLED, false);
    int llapMemory = 0;
    int llapCpus = 0;
    DAGAPI.DaemonVertexProto llapDaemonVertexProto = null;
    if (useLlapIo) {
      // llapMemory = 0 and llapCpus = 0 are valid.
      llapMemory = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_DAEMON_TASK_MEMORY_MB);
      if (llapMemory < 0) {
        llapMemory = defaultLlapDaemonTaskMemoryMb;
      }
      llapCpus = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_DAEMON_TASK_VCORES);
      if (llapCpus < 0) {
        llapCpus = defaultLlapDaemonTaskVcores;
      }
      llapDaemonVertexProto = createLlapDaemonVertexProto(conf, llapMemory, llapCpus);
    }

    // we do not create containerGroupConf
    List<DAGAPI.ContainerGroupProto> containerGroupProtos =
        createContainerGroupProtos(conf, scheme, vertices.values(),
            llapMemory, llapCpus, llapDaemonVertexProto);

    DAGAPI.ConfigurationProto dagConfProto = Utils$.MODULE$.createMr3ConfProto(dagConf);

    // We should call setDagConf(). Otherwise we would end up using DAGAppMaster.MR3Conf in MR3.
    DAGAPI.DAGProto dagProto = DAGAPI.DAGProto.newBuilder()
        .setName(name)
        .setCredentials(CommonUtils.convertCredentialsToByteString(dagCredentials))
        .setDagInfo(dagInfo)
        .addAllVertices(vertexProtos)
        .addAllEdges(edgeProtos)
        .addAllVertexGroups(vertexGroupProtos)
        .addAllLocalResources(lrProtos)
        .addAllContainerGroups(containerGroupProtos)
        .setDagConf(dagConfProto)
        .build();

    return dagProto;
  }

  private ContainerGroupScheme getContainerGroupScheme(Configuration conf) {
    String scheme = conf.get(HiveConf.ConfVars.MR3_CONTAINERGROUP_SCHEME.varname);
    if (scheme.equals("all-in-one")) {
      return ContainerGroupScheme.ALL_IN_ONE;
    } else if (scheme.equals("per-map-reduce")) {
      return ContainerGroupScheme.PER_MAP_REDUCE;
    } else {  // defaults to "per-vertex"
      return ContainerGroupScheme.PER_VERTEX;
    }
  }

  private List<DAGAPI.VertexProto> createVertexProtos(ContainerGroupScheme scheme) {
    List<DAGAPI.VertexProto> vertexProtos = new ArrayList<DAGAPI.VertexProto>();

    for (Vertex vertex: vertices.values()) {
      dagCredentials.addAll(vertex.getAggregatedCredentials());
      String containerGroupName = vertex.getContainerGroupName(scheme);
      vertexProtos.add(vertex.createVertexProto(containerGroupName));
    }

    return vertexProtos;
  }

  private DAGAPI.DaemonVertexProto createLlapDaemonVertexProto(
      Configuration conf,
      int llapMemory, int llapCpus) throws IOException {
    DAGAPI.ResourceProto resource = DAGAPI.ResourceProto.newBuilder()
        .setMemoryMb(llapMemory)
        .setVirtualCores(llapCpus)
        .build();

    String procClassName = LLAPDaemonProcessor.class.getName();
    ByteString userPayload = org.apache.tez.common.TezUtils.createByteStringFromConf(conf);
    EntityDescriptor processorDescriptor = new EntityDescriptor(procClassName, userPayload);

    String pluginClassName = LLAPDaemonVertexManagerPlugin.class.getName();
    EntityDescriptor vertexManagerPluginDescriptor = new EntityDescriptor(pluginClassName, null);

    DAGAPI.DaemonVertexProto daemonVertexProto = DAGAPI.DaemonVertexProto.newBuilder()
        .setName("LLAP")
        .setResource(resource)
        .setProcessor(processorDescriptor.createEntityDescriptorProto())
        .setVertexManagerPlugin(vertexManagerPluginDescriptor.createEntityDescriptorProto())
        .build();

    return daemonVertexProto;
  }

  private List<DAGAPI.ContainerGroupProto> createContainerGroupProtos(
      Configuration conf, ContainerGroupScheme scheme, Collection<Vertex> vertices,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto) {
    List<DAGAPI.ContainerGroupProto> containerGroupProtos = new ArrayList<DAGAPI.ContainerGroupProto>();

    if (scheme == DAG.ContainerGroupScheme.ALL_IN_ONE) {
      DAGAPI.ContainerGroupProto allInOneContainerGroupProto =
        createAllInOneContainerGroupProto(conf, llapMemory, llapCpus, llapDaemonVertexProto);
      containerGroupProtos.add(allInOneContainerGroupProto);

    } else if (scheme == DAG.ContainerGroupScheme.PER_MAP_REDUCE) {
      DAGAPI.ContainerGroupProto perMapContainerGroupProto =
        createPerMapReduceContainerGroupProto(conf, true, llapMemory, llapCpus, llapDaemonVertexProto);
      DAGAPI.ContainerGroupProto perReduceContainerGroupProto =
        createPerMapReduceContainerGroupProto(conf, false, 0, 0, null);
      containerGroupProtos.add(perMapContainerGroupProto);
      containerGroupProtos.add(perReduceContainerGroupProto);

    } else {
      for(Vertex vertex: vertices) {
        DAGAPI.ContainerGroupProto perVertexContainerGroupProto =
          createPerVertexContainerGroupProto(conf, vertex);
        containerGroupProtos.add(perVertexContainerGroupProto);
      }
    }

    return containerGroupProtos;
  }

  private DAGAPI.ContainerGroupProto createAllInOneContainerGroupProto(Configuration conf,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto) {
    int llapNativeMemoryMb = 0;
    if (llapDaemonVertexProto != null) {
      long ioMemoryBytes = HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      int headroomMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_HEADROOM_MB);
      llapNativeMemoryMb = (int)(ioMemoryBytes >> 20) + headroomMb;
    }

    int allLlapMemory = llapMemory + llapNativeMemoryMb;
    DAGAPI.ResourceProto allInOneResource =
      createResourceProto(DAGUtils.getAllInOneContainerGroupResource(conf, allLlapMemory, llapCpus));

    DAGAPI.ContainerConfigurationProto.Builder allInOneContainerConf =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(allInOneResource);
    setJavaOptsEnvironmentStr(conf, allInOneContainerConf);

    if (llapDaemonVertexProto != null) {
      allInOneContainerConf.setNativeMemoryMb(llapNativeMemoryMb);
    }

    DAGAPI.ContainerGroupProto.Builder allInOneContainerGroup =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(ALL_IN_ONE_CONTAINER_GROUP_NAME)
          .setContainerConfig(allInOneContainerConf.build())
          .setPriority(allInOneContainerGroupPriority)
          .setContainerGroupConf(getContainerGroupConfProto(conf));
    if (llapDaemonVertexProto != null) {
      List<DAGAPI.DaemonVertexProto> daemonVertexProtos = Collections.singletonList(llapDaemonVertexProto);
      allInOneContainerGroup.addAllDaemonVertices(daemonVertexProtos);
    }

    return allInOneContainerGroup.build();
  }

  private DAGAPI.ContainerGroupProto createPerMapReduceContainerGroupProto(
      Configuration conf, boolean isMap,
      int llapMemory, int llapCpus, DAGAPI.DaemonVertexProto llapDaemonVertexProto) {
    String groupName = isMap ? PER_MAP_CONTAINER_GROUP_NAME : PER_REDUCE_CONTAINER_GROUP_NAME;
    int priority = isMap ? perMapContainerGroupPriority : perReduceContainerGroupPriority;

    int llapNativeMemoryMb = 0;
    if (isMap && llapDaemonVertexProto != null) {
      long ioMemoryBytes = HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      int headroomMb = HiveConf.getIntVar(conf, HiveConf.ConfVars.MR3_LLAP_HEADROOM_MB);
      llapNativeMemoryMb = (int)(ioMemoryBytes >> 20) + headroomMb;
    }

    int allLlapMemory = llapMemory + llapNativeMemoryMb;
    Resource resource =
      isMap ?
          DAGUtils.getMapContainerGroupResource(conf, allLlapMemory, llapCpus) :
          DAGUtils.getReduceContainerGroupResource(conf);
    DAGAPI.ResourceProto perMapReduceResource = createResourceProto(resource);

    DAGAPI.ContainerConfigurationProto.Builder perMapReduceContainerConf =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(perMapReduceResource);
    setJavaOptsEnvironmentStr(conf, perMapReduceContainerConf);

    DAGAPI.ContainerGroupProto.Builder perMapReduceContainerGroup =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(groupName)
          .setContainerConfig(perMapReduceContainerConf.build())
          .setPriority(priority)
          .setContainerGroupConf(getContainerGroupConfProto(conf));
    if (isMap && llapDaemonVertexProto != null) {
      List<DAGAPI.DaemonVertexProto> daemonVertexProtos = Collections.singletonList(llapDaemonVertexProto);
      perMapReduceContainerGroup.addAllDaemonVertices(daemonVertexProtos);
    }

    return perMapReduceContainerGroup.build();
  }

  private DAGAPI.ContainerGroupProto createPerVertexContainerGroupProto(
      Configuration conf, Vertex vertex) {
    int priority = vertex.getDistanceFromRoot() * 3;

    Resource resource =
      vertex.isMapVertex() ?
          DAGUtils.getMapContainerGroupResource(conf, 0, 0) :
          DAGUtils.getReduceContainerGroupResource(conf);
    DAGAPI.ResourceProto vertexResource = createResourceProto(resource);

    DAGAPI.ContainerConfigurationProto.Builder containerConfig =
      DAGAPI.ContainerConfigurationProto.newBuilder()
          .setResource(vertexResource);
    String javaOpts = vertex.getContainerJavaOpts();
    if (javaOpts != null) {
      containerConfig.setJavaOpts(javaOpts);
    }
    String environmentStr = vertex.getContainerEnvironment();
    if (environmentStr != null) {
      containerConfig.setEnvironmentStr(environmentStr);
    }

    DAGAPI.ContainerGroupProto perVertexContainerGroupProto =
      DAGAPI.ContainerGroupProto.newBuilder()
          .setName(vertex.getName())
          .setContainerConfig(containerConfig.build())
          .setPriority(priority)
          .setContainerGroupConf(getContainerGroupConfProto(conf))
          .build();

    return perVertexContainerGroupProto;
  }

  private DAGAPI.ConfigurationProto getContainerGroupConfProto(Configuration conf) {
    boolean combineTaskAttempts = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_COMBINE_TASKATTEMPTS);
    boolean containerReuse = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_REUSE);
    boolean mixTaskAttempts = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MR3_CONTAINER_MIX_TASKATTEMPTS);
    MR3Conf containerGroupConf = new MR3ConfBuilder(false)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_COMBINE_TASKATTEMPTS(), combineTaskAttempts)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_REUSE(), containerReuse)
        .setBoolean(MR3Conf$.MODULE$.MR3_CONTAINER_MIX_TASKATTEMPTS(), mixTaskAttempts)
        .build();

    return Utils$.MODULE$.createMr3ConfProto(containerGroupConf);
  }

  private void setJavaOptsEnvironmentStr(
      Configuration conf,
      DAGAPI.ContainerConfigurationProto.Builder containerConf) {
    String javaOpts = DAGUtils.getContainerJavaOpts(conf);
    if (javaOpts != null) {
      containerConf.setJavaOpts(javaOpts);
    }

    String environmentStr = DAGUtils.getContainerEnvironment(conf);
    if (environmentStr != null) {
      containerConf.setEnvironmentStr(environmentStr);
    }
  }

  private DAGAPI.ResourceProto createResourceProto(Resource resource) {
    return
      DAGAPI.ResourceProto.newBuilder()
          .setMemoryMb(resource.getMemory())
          .setVirtualCores(resource.getVirtualCores())
          .build();
  }
}
