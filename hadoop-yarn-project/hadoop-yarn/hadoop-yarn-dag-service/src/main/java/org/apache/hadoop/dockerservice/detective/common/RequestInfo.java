package org.apache.hadoop.dockerservice.detective.common;

import org.apache.hadoop.dockerservice.detective.graph.Node;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RequestInfo {
  private final String url;
  private final String requestId;
  private final long totalTime;
  private final Map<String,Long> serviceMap;

  public static RequestInfo getRequestInfo(String url, String requestId, List<Node> nodes){
    if ( nodes == null || url == null || requestId == null ){
      throw new IllegalArgumentException();
    }

    long totalTime = 0;
    Map<String,Long> map = new HashMap<>();
    for (Node node : nodes){
      if ( node.getType() != Node.Type.NETWORK ){
        //这里总时间是不计算网络的
        totalTime += node.getTime();
        String name = node.getServiceName();
        long time = node.getTime();
        if ( !map.keySet().contains(name) ){
          map.put(name,time);
        }else {
          map.put(name,time + map.get(name));
        }
      }
    }

    return new RequestInfo(url,requestId,totalTime,map);
  }

  private RequestInfo(String url, String requestId, long totalTime, Map<String,Long> map){
    this.url = url;
    this.requestId = requestId;
    this.totalTime = totalTime;
    this.serviceMap = map;
  }

  public String getUrl() {
    return url;
  }

  public long getTotalTime() {
    return totalTime;
  }

  public Map<String, Long> getServiceMap() {
    return serviceMap;
  }

  public String getRequestId() {
    return requestId;
  }
}
