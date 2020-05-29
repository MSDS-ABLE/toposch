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

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.impl.pb.*;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceBlacklistRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerScoreProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerScoreProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.UpdateContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class AllocateRequestPBImpl extends AllocateRequest {
  AllocateRequestProto proto = AllocateRequestProto.getDefaultInstance();
  AllocateRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<ResourceRequest> ask = null;
  private List<ContainerId> release = null;
  private List<UpdateContainerRequest> updateRequests = null;
  private ResourceBlacklistRequest blacklistRequest = null;

  //added by junqing xiao 18.4.16
  private List<ContainerScore> scores = null;
  
  public AllocateRequestPBImpl() {
    builder = AllocateRequestProto.newBuilder();
  }

  public AllocateRequestPBImpl(AllocateRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  public AllocateRequestProto getProto() {
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.ask != null) {
      addAsksToProto();
    }
    if (this.release != null) {
      addReleasesToProto();
    }
    if (this.updateRequests != null) {
      addUpdateRequestsToProto();
    }
    //added by junqing xiao 18.4.16
    if ( this.scores != null ){
      addScoresToProto();
    }
    if (this.blacklistRequest != null) {
      builder.setBlacklistRequest(convertToProtoFormat(this.blacklistRequest));
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
      builder = AllocateRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public int getResponseId() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getResponseId();
  }

  @Override
  public void setResponseId(int id) {
    maybeInitBuilder();
    builder.setResponseId(id);
  }

  @Override
  public float getProgress() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getProgress();
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress(progress);
  }

  @Override
  public List<ResourceRequest> getAskList() {
    initAsks();
    return this.ask;
  }
  
  @Override
  public void setAskList(final List<ResourceRequest> resourceRequests) {
    if(resourceRequests == null) {
      return;
    }
    initAsks();
    this.ask.clear();
    this.ask.addAll(resourceRequests);
  }
  
  @Override
  public List<UpdateContainerRequest> getUpdateRequests() {
    initUpdateRequests();
    return this.updateRequests;
  }
  //added by junqing xiao 18.4.16
  @Override
  public List<ContainerScore> getScores(){
    initScores();
    return this.scores;
  }

  @Override
  public void setUpdateRequests(List<UpdateContainerRequest> updateRequests) {
    if (updateRequests == null) {
      return;
    }
    initUpdateRequests();
    this.updateRequests.clear();
    this.updateRequests.addAll(updateRequests);
  }

  //added by junqing xiao 18.4.16
  @Override
  public void setScores(List<ContainerScore> scores) {
    if (scores == null) {
      return;
    }
    initScores();
    this.scores.clear();
    this.scores.addAll(scores);
  }

  @Override
  public ResourceBlacklistRequest getResourceBlacklistRequest() {
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.blacklistRequest != null) {
      return this.blacklistRequest;
    }
    if (!p.hasBlacklistRequest()) {
      return null;
    }
    this.blacklistRequest = convertFromProtoFormat(p.getBlacklistRequest());
    return this.blacklistRequest;
  }

  @Override
  public void setResourceBlacklistRequest(ResourceBlacklistRequest blacklistRequest) {
    maybeInitBuilder();
    if (blacklistRequest == null) {
      builder.clearBlacklistRequest();
    }
    this.blacklistRequest = blacklistRequest;
  }

  private void initAsks() {
    if (this.ask != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ResourceRequestProto> list = p.getAskList();
    this.ask = new ArrayList<ResourceRequest>();

    for (ResourceRequestProto c : list) {
      this.ask.add(convertFromProtoFormat(c));
    }
  }
  
  private void addAsksToProto() {
    maybeInitBuilder();
    builder.clearAsk();
    if (ask == null)
      return;
    Iterable<ResourceRequestProto> iterable =
        new Iterable<ResourceRequestProto>() {
      @Override
      public Iterator<ResourceRequestProto> iterator() {
        return new Iterator<ResourceRequestProto>() {

          Iterator<ResourceRequest> iter = ask.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceRequestProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllAsk(iterable);
  }
  
  private void initUpdateRequests() {
    if (this.updateRequests != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<UpdateContainerRequestProto> list =
        p.getUpdateRequestsList();
    this.updateRequests = new ArrayList<>();

    for (UpdateContainerRequestProto c : list) {
      this.updateRequests.add(convertFromProtoFormat(c));
    }
  }

  //added by junqing xiao 18.4.16
  private void initScores() {
    if (this.scores != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerScoreProto> list =
            p.getScoresList();
    this.scores = new ArrayList<>();

    for (ContainerScoreProto c : list) {
      this.scores.add(convertFromProtoFormat(c));
    }
  }

  private void addUpdateRequestsToProto() {
    maybeInitBuilder();
    builder.clearUpdateRequests();
    if (updateRequests == null) {
      return;
    }
    Iterable<UpdateContainerRequestProto> iterable =
        new Iterable<UpdateContainerRequestProto>() {
          @Override
          public Iterator<UpdateContainerRequestProto> iterator() {
            return new Iterator<UpdateContainerRequestProto>() {

              private Iterator<UpdateContainerRequest> iter =
                  updateRequests.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public UpdateContainerRequestProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };

          }
        };
    builder.addAllUpdateRequests(iterable);
  }

  //added by junqing xiao 18.4.16
  private void addScoresToProto() {
    maybeInitBuilder();
    builder.clearScores();
    if (scores == null) {
      return;
    }
    Iterable<ContainerScoreProto> iterable =
            new Iterable<ContainerScoreProto>() {
              @Override
              public Iterator<ContainerScoreProto> iterator() {
                return new Iterator<ContainerScoreProto>() {

                  private Iterator<ContainerScore> iter =
                          scores.iterator();

                  @Override
                  public boolean hasNext() {
                    return iter.hasNext();
                  }

                  @Override
                  public ContainerScoreProto next() {
                    return convertToProtoFormat(iter.next());
                  }

                  @Override
                  public void remove() {
                    throw new UnsupportedOperationException();
                  }
                };

              }
            };
    builder.addAllScores(iterable);
  }

  @Override
  public List<ContainerId> getReleaseList() {
    initReleases();
    return this.release;
  }
  @Override
  public void setReleaseList(List<ContainerId> releaseContainers) {
    if(releaseContainers == null) {
      return;
    }
    initReleases();
    this.release.clear();
    this.release.addAll(releaseContainers);
  }
  
  private void initReleases() {
    if (this.release != null) {
      return;
    }
    AllocateRequestProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getReleaseList();
    this.release = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.release.add(convertFromProtoFormat(c));
    }
  }
  
  private void addReleasesToProto() {
    maybeInitBuilder();
    builder.clearRelease();
    if (release == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = release.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllRelease(iterable);
  }

  private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
    return new ResourceRequestPBImpl(p);
  }

  private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
    return ((ResourceRequestPBImpl)t).getProto();
  }
  
  private UpdateContainerRequestPBImpl convertFromProtoFormat(
      UpdateContainerRequestProto p) {
    return new UpdateContainerRequestPBImpl(p);
  }

  //added by junqing xiao 18.4.16
  private ContainerScorePBImpl convertFromProtoFormat(
          ContainerScoreProto p) {
    return new ContainerScorePBImpl(p);
  }

  private UpdateContainerRequestProto convertToProtoFormat(
      UpdateContainerRequest t) {
    return ((UpdateContainerRequestPBImpl) t).getProto();
  }

  //added by junqing xiao 18.4.16
  private ContainerScoreProto convertToProtoFormat(
          ContainerScore t) {
    return ((ContainerScorePBImpl) t).getProto();
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  private ResourceBlacklistRequestPBImpl convertFromProtoFormat(ResourceBlacklistRequestProto p) {
    return new ResourceBlacklistRequestPBImpl(p);
  }

  private ResourceBlacklistRequestProto convertToProtoFormat(ResourceBlacklistRequest t) {
    return ((ResourceBlacklistRequestPBImpl)t).getProto();
  }
}  
