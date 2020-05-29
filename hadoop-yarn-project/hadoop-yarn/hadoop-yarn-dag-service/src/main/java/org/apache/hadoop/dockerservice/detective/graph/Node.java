package org.apache.hadoop.dockerservice.detective.graph;

import com.gaea.common.Trace;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * serviceName和Stage唯一标识一个节点
 */
public class Node {
  public static final String CLIENT_NAME = "client";
  public static final String NETWORK_NAME = "network";
  public static final String stageSeparator = ":";

  public enum Type {
    SERVICE, NETWORK, AGGREGATE
  }

  private String serviceName;
  private String stage;
  private Type type;
  private List<Node> source = new ArrayList<>();
  private List<Node> destination = new ArrayList<>();
  private long time;

  private long startTime;
  private long endTime;

  @VisibleForTesting
  public Node(String stage, long time){
    this.stage = stage;
    this.time = time;
  }

  //客户端节点
  public static final Node clientNode = new Node(CLIENT_NAME, Type.SERVICE);

  public static Node generateFromTrace(Trace trace) {
    return new Node(trace.getServiceName(), trace.getStage());
  }

  public static Node generateNetworkNode(){
    return new Node(NETWORK_NAME, Node.Type.NETWORK);
  }

  public static Node generateNetworkNode(Node outNode,Node inNode){
    Node node = new Node(NETWORK_NAME, Type.NETWORK);
    inNode.addSource(node);
    outNode.addDestination(node);
    node.setStage(UUID.randomUUID().toString());

    //聚合节点前网络时间是0
    if ( inNode.getType() != Type.AGGREGATE ){
      node.setTime(inNode.startTime - outNode.endTime);
    }else {
      node.setTime(0);
    }

    inNode.deleteSource(outNode);
    outNode.deleteDestination(inNode);
    return node;
  }


  private Node(String serviceName, Type type) {
    this.serviceName = serviceName;
    this.type = type;
  }

  private Node(String serviceName, String stage) {
    this.serviceName = serviceName;
    this.stage = stage;
    this.type = Type.SERVICE;
  }

  public final boolean checkMiss() {
    if ( CLIENT_NAME.equals(serviceName) ) {
      return true;
    }
    if ( source.size() == 0 || destination.size() == 0 ) {
      return false;
    }

    time = endTime - startTime;
    return time >= 0;
  }

  @Override
  public String toString(){
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("stage = ").append(stage).append(",").
            append("type = ").append(type).append(",").
            append("serviceName = ").append(serviceName).append(",");
    for (Node node  : source){
      stringBuilder.append("source = <").append(node.getStage()).append(" ");
    }
    stringBuilder.append(">");

    for (Node node  : destination){
      stringBuilder.append("des = <").append(node.getStage()).append(" ");
    }
    stringBuilder.append(">");

    return stringBuilder.toString();
  }

  public Type getType(){
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getStage() {
    return stage;
  }

  public void setStage(String stage) {
    this.stage = stage;
  }

  public List<Node> getSource() {
    return source;
  }

  public void addSource(Node source) {
    this.source.add(source);
  }

  public void deleteSource(Node source) {
    this.source.remove(source);
  }

  public List<Node> getDestination() {
    return destination;
  }

  public void addDestination(Node destination) {
    this.destination.add(destination);
  }

  public void deleteDestination(Node destination) {
    this.destination.remove(destination);
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getTime() {
    return time;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }
}
