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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ExecutionTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.MixTypeProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ServicePortProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ServicePortProtoOrBuilder;
@Private
@Unstable
public class ContainerPBImpl extends Container {

  ContainerProto proto = ContainerProto.getDefaultInstance();
  ContainerProto.Builder builder = null;
  boolean viaProto = false;
  
  private ContainerId containerId = null;
  private NodeId nodeId = null;
  private Resource resource = null;
  //AM-Orchestration ZJY
  private ServicePort ServicePort;
  private Priority priority = null;
  private Token containerToken = null;
  //added by Junqing Xiao 18.1.8
  private MixType mixType = null;

  public ContainerPBImpl() {
    builder = ContainerProto.newBuilder();
  }

  public ContainerPBImpl(ContainerProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public ContainerProto getProto() {
  
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
            builder.getId())) {
      builder.setId(convertToProtoFormat(this.containerId));
    }
    if (this.nodeId != null
        && !((NodeIdPBImpl) nodeId).getProto().equals(
            builder.getNodeId())) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.mixType != null) {
      builder.setMixType(convertToProtoFormat(this.mixType));
    }
    if (this.priority != null && 
        !((PriorityPBImpl) this.priority).getProto().equals(
            builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.containerToken != null
        && !((TokenPBImpl) this.containerToken).getProto().equals(
            builder.getContainerToken())) {
      builder.setContainerToken(convertToProtoFormat(this.containerToken));
    }
    //AM-Orchestration ZJY
    if (this.ServicePort != null){
      builder.setServicePort(this.convertToProtoFormat(this.ServicePort));
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ContainerId getId() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerId != null) {
      return this.containerId;
    }
    if (!p.hasId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getId());
    return this.containerId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null)
      builder.clearNodeId();
    this.nodeId = nodeId;
  }

  @Override
  public NodeId getNodeId() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setId(ContainerId id) {
    maybeInitBuilder();
    if (id == null)
      builder.clearId();
    this.containerId = id;
  }

  @Override
  public String getNodeHttpAddress() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeHttpAddress()) {
      return null;
    }
    return (p.getNodeHttpAddress());
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    maybeInitBuilder();
    if (nodeHttpAddress == null) {
      builder.clearNodeHttpAddress();
      return;
    }
    builder.setNodeHttpAddress(nodeHttpAddress);
  }

  @Override
  public Resource getResource() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  /**
   * AM-Orchestration ZJY
   * */
  @Override
  public ServicePort getServicePort() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.ServicePort != null) {
      return this.ServicePort;
    }
    if (!p.hasServicePort()) {
      return null;
    }
    this.ServicePort =convertFromProtoFormat(p.getServicePort());
    return this.ServicePort;
  }

  @Override
  public void setServicePort(ServicePort servicePort) {
    maybeInitBuilder();
    if (ServicePort == null)
      builder.clearServicePort();
    this.ServicePort = servicePort;
  }
  @Override
  public Priority getPriority() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearPriority();
    }
    this.priority = priority;
  }

  //added by Junqing Xiao 18.1.8
  @Override
  public MixType getMixType() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.mixType != null) {
      return this.mixType;
    }
    if (!p.hasMixType()) {
      return null;
    }
    this.mixType = convertFromProtoFormat(p.getMixType());
    return this.mixType;
  }

  @Override
  public void setMixType(MixType mixType) {
    maybeInitBuilder();
    if (mixType == null) {
      builder.clearMixType();
    }
    this.mixType = mixType;
  }

  @Override
  public Token getContainerToken() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerToken != null) {
      return this.containerToken;
    }
    if (!p.hasContainerToken()) {
      return null;
    }
    this.containerToken = convertFromProtoFormat(p.getContainerToken());
    return this.containerToken;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    maybeInitBuilder();
    if (containerToken == null) 
      builder.clearContainerToken();
    this.containerToken = containerToken;
  }

  @Override
  public ExecutionType getExecutionType() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    return convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public void setExecutionType(ExecutionType executionType) {
    maybeInitBuilder();
    builder.setExecutionType(convertToProtoFormat(executionType));
  }

  @Override
  public long getAllocationRequestId() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAllocationRequestId());
  }

  @Override
  public void setAllocationRequestId(long allocationRequestID) {
    maybeInitBuilder();
    builder.setAllocationRequestId(allocationRequestID);
  }

  @Override
  public int getVersion() {
    ContainerProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVersion();
  }

  @Override
  public void setVersion(int version) {
    maybeInitBuilder();
    builder.setVersion(version);
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl)t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  //AM-Orchestration ZJY
  private ServicePortPBImpl convertFromProtoFormat(ServicePortProto p){
    return new ServicePortPBImpl(p);
  }
  private ServicePortProto convertToProtoFormat(ServicePort port){
    return ProtoUtils.convertToProtoFormat(port);
  }



  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority p) {
    return ((PriorityPBImpl)p).getProto();
  }
  
  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }

  //added by Junqing Xiao 18.1.8
  private MixType convertFromProtoFormat(MixTypeProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }
  private MixTypeProto convertToProtoFormat(MixType e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  private ExecutionType convertFromProtoFormat(
      ExecutionTypeProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private ExecutionTypeProto convertToProtoFormat(ExecutionType e) {
    return ProtoUtils.convertToProtoFormat(e);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Container: [");
    sb.append("ContainerId: ").append(getId()).append(", ");
    sb.append("AllocationRequestId: ").append(getAllocationRequestId())
        .append(", ");
    sb.append("Version: ").append(getVersion()).append(", ");
    sb.append("NodeId: ").append(getNodeId()).append(", ");
    sb.append("NodeHttpAddress: ").append(getNodeHttpAddress()).append(", ");
    sb.append("Resource: ").append(getResource()).append(", ");
    sb.append("Priority: ").append(getPriority()).append(", ");
    sb.append("Token: ").append(getContainerToken()).append(", ");
    sb.append("ExecutionType: ").append(getExecutionType()).append(", ");
    sb.append("MixType: ").append(getMixType()).append(", ");
    sb.append("]");
    return sb.toString();
  }

  //TODO Comparator
  @Override
  public int compareTo(Container other) {
    if (this.getId().compareTo(other.getId()) == 0) {
      if (this.getNodeId().compareTo(other.getNodeId()) == 0) {
        return this.getResource().compareTo(other.getResource());
      } else {
        return this.getNodeId().compareTo(other.getNodeId());
      }
    } else {
      return this.getId().compareTo(other.getId());
    }
  }

}
