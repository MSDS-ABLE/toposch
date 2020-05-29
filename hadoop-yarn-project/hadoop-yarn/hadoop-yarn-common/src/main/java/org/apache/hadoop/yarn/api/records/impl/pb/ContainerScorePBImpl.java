package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerScore;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerScoreProto;

import static org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils.convertFromProtoFormat;

/**
 * Created by Junqing xiao 18.4.16
 */

public class ContainerScorePBImpl extends ContainerScore {
  ContainerScoreProto proto = ContainerScoreProto.getDefaultInstance();
  ContainerScoreProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId containerId = null;
  private double score;

  public ContainerScorePBImpl(){
    builder = ContainerScoreProto.newBuilder();
  }

  public ContainerScorePBImpl(ContainerScoreProto proto){
    this.proto = proto;
    viaProto = true;
  }

  public ContainerScoreProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void mergeLocalToBuilder() {
    if (this.containerId != null
            && !((ContainerIdPBImpl) containerId).getProto().equals(
            builder.getId())) {
      builder.setId(convertToProtoFormat(this.containerId));
    }
    builder.setScore(score);
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerScoreProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private YarnProtos.ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  @Override
  public ContainerId getId() {
    YarnProtos.ContainerScoreProtoOrBuilder p = viaProto ? proto : builder;
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
  public void setId(ContainerId id) {
    maybeInitBuilder();
    if (id == null)
      builder.clearId();
    this.containerId = id;
  }

  @Override
  public double getScore() {
    YarnProtos.ContainerScoreProtoOrBuilder p = viaProto ? proto : builder;
    return p.getScore();
  }

  @Override
  public void setScore(double score) {
    maybeInitBuilder();
    builder.setScore(score);
    this.score = score;
  }

}
