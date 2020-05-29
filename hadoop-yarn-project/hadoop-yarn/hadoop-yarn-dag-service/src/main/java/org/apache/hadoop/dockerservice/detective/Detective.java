package org.apache.hadoop.dockerservice.detective;

import com.alibaba.fastjson.JSON;
import com.gaea.common.EventType;
import com.gaea.common.StatusCode;
import com.gaea.common.Trace;
import org.apache.hadoop.dockerservice.detective.common.RequestInfo;
import org.apache.hadoop.dockerservice.detective.graph.Node;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.apache.hadoop.dockerservice.detective.graph.Node.CLIENT_NAME;
import static org.apache.hadoop.dockerservice.detective.graph.Node.clientNode;

public class Detective {
  private Log LOG = LogFactory.getLog(Detective.class);
  private static final String TRACE_KEY_NAME = "trace";
  private static final String HOST = "127.0.0.1";

  private static final double TIMEWEIGHT = 0.7;
  private static final double UNRELIABLEWEIGHT = 0.3;

  //<service,<url,num>>
  private Map<String,Map<String,Long>> urlErrorMap = new HashMap<>();

  /**
   * 计算函数主入口
   */
  public Map<String, Double> compute() {
    //0.获取信息
    Map<String, Double> scoreMap = null;
    try {
      Jedis jedis = new Jedis(HOST);
      Long length = jedis.llen(TRACE_KEY_NAME);
      LOG.info("***********************************************************************");
      LOG.info("read " + length + " trace");
      Map<String, Map<String, List<Trace>>> map = new HashMap<>();
      //map中是<url,<requestID,List<Trace>>>结构
      for (long i = 0; i < length; i++) {
        Trace trace = JSON.parseObject(jedis.lpop(TRACE_KEY_NAME), Trace.class);
        //***特殊逻辑***    忽略掉/token结尾 和 url = null 的
        String url = trace.getUrl();
        if ( url == null ) {
          continue;
        } else {
          String[] strings = url.split("/");
          if ( "token".equals(strings[strings.length - 1]) ) {
            continue;
          }
        }

        Map<String, List<Trace>> urlMap = map.get(trace.getUrl());
        if ( urlMap == null ) {
          urlMap = new HashMap<>();
          map.put(trace.getUrl(), urlMap);
        }
        List<Trace> list = urlMap.get(trace.getRequestId());
        if ( list == null ) {
          list = new ArrayList<>();
          urlMap.put(trace.getRequestId(), list);
        }
        list.add(trace);
      }

      StringBuilder stringBuilder = new StringBuilder("junqing xiao : urlMap-->{");

      for (String url : map.keySet()){
        stringBuilder.append("<").append(url).append(">,");
      }
      stringBuilder.append("}");
      LOG.info(stringBuilder.toString());

      Map<String, List<RequestInfo>> cpMap = new HashMap<>();
      Map<String, Long> timeMap = new HashMap<>();

      int totalRequestWeight = 0;
      Set<String> serviceSet = new HashSet<>();
      scoreMap = new HashMap<>();

      //1.为每个请求生成调用关系图
      for (Map.Entry<String, Map<String, List<Trace>>> mapEntry : map.entrySet()) {
        String url = mapEntry.getKey();
        long time = 0;
        Map<String, List<Trace>> urlMap = mapEntry.getValue();
        List<RequestInfo> requestInfos = new ArrayList<>();
        for (Map.Entry<String, List<Trace>> urlMapEntry : urlMap.entrySet()) {
          List<Trace> list = urlMapEntry.getValue();
          String requestId = urlMapEntry.getKey();
          List<Node> nodes = generateGraph(requestId, list);
          if ( nodes == null ) {
            continue;
          }
          //1.2计算关键路径
          //RequestInfo info = RequestInfo.getRequestInfo(url, requestId, findCP(nodes));
          nodes.remove(Node.clientNode);
          RequestInfo info = RequestInfo.getRequestInfo(url, requestId, nodes);
          requestInfos.add(info);

          time += info.getTotalTime();
          totalRequestWeight += info.getTotalTime();
          serviceSet.addAll(info.getServiceMap().keySet());
        }
        timeMap.put(url, time);
        cpMap.put(url, requestInfos);
      }
      LOG.info("complete find cp");
      //为每个service算分
      for (String service : serviceSet) {

        StringBuilder sb = new StringBuilder("junqing xiao : detective compute-->{service = " + service);

        for (String url : cpMap.keySet()) {
          double weight = timeMap.get(url) * 1.0 / totalRequestWeight;
          sb.append("{url = ").append(url).append(" weight = ").append(weight);
          long serviceTime = 0;
          long totalTime = 0;
          for (RequestInfo info : cpMap.get(url)) {
            if ( info.getServiceMap().containsKey(service) ) {
              serviceTime += info.getServiceMap().get(service);
            }
            totalTime += info.getTotalTime();
          }
          double timeScore = serviceTime * 1.0 / totalTime;
          sb.append("serviceTime = ").append(serviceTime)
                  .append("totalTime = ").append(totalTime)
                  .append("timeScore = ").append(timeScore)
                  .append("}");
        }
        sb.append("}");
        LOG.info(sb.toString());

        double score = 0;
        for (String url : cpMap.keySet()) {
          if ( cpMap.get(url).size() > 0 ) {
            double weight = timeMap.get(url) * 1.0 / totalRequestWeight;
            //每个url内的算分
            //1.   TIMEWEIGHT 时间 / 总时间
            long serviceTime = 0;
            long totalTime = 0;
            long serviceNum = 0;
            for (RequestInfo info : cpMap.get(url)) {
              if ( info.getServiceMap().containsKey(service) ) {
                serviceTime += info.getServiceMap().get(service);
                serviceNum ++;
              }
              totalTime += info.getTotalTime();
            }
            double timeScore = serviceTime * 1.0 / totalTime;
            //2.   UNRELIABLEWEIGHT 不可靠性
            double unreliableScore = 0;
            if ( urlErrorMap.containsKey(service) ){
              if ( urlErrorMap.get(service).containsKey(url) ){
                unreliableScore = urlErrorMap.get(service).get(url) * 1.0 / serviceNum;
              }
            }
            if ( unreliableScore != 0 ){
              LOG.warn("unreliableScore = " + unreliableScore + ",service : " + service + ",url : " + url);
            }
            //3.综合计算得分
            score += weight * (TIMEWEIGHT * timeScore + UNRELIABLEWEIGHT * unreliableScore);
          }
        }
        scoreMap.put(service, score);
      }
      LOG.info("complete scoreMap");
    }catch (Throwable e){
      for (StackTraceElement element : e.getStackTrace()){
        LOG.info(element.toString());
      }
    }

    return scoreMap;
  }

  @VisibleForTesting
  List<Node> findCP(List<Node> nodes) {
    LOG.info("in find cp");

    //1.算出源节点和目标节点
    Node firstNode = null;
    Node lastNode = null;
    for (Node node : nodes) {
      if ( node.getSource().contains(clientNode) ) {
        firstNode = node;
      }
      if ( node.getDestination().contains(clientNode) ) {
        lastNode = node;
      }
    }
    //2.遍历计算关键路径
    Stack<Node> stack = new Stack<>();
    stack.push(firstNode);
    Map<String, Integer> choiceMap = new HashMap<>();
    Node currentNode = firstNode;
    long max = 0;
    List<Node> maxNodes = new ArrayList<>();
    assert currentNode != null;
    long length = currentNode.getTime();

    while (!stack.empty()) {
      //计算某一次长度
      while (currentNode != lastNode) {
        int num = 0;
        String stage = currentNode.getStage();
        if ( !choiceMap.containsKey(stage) ) {
          choiceMap.put(stage, num);
        } else {
          num = choiceMap.get(stage) + 1;
          choiceMap.put(stage, num);
        }
        try {
          currentNode = currentNode.getDestination().get(num);
        }catch (Throwable e){
          LOG.info(e.getStackTrace());
          for (Node node : nodes){
            LOG.info(node);
          }
        }
        stack.push(currentNode);
        length += currentNode.getTime();
      }
      if ( length > max ) {
        max = length;
        //保存元素
        maxNodes.clear();
        maxNodes.addAll(stack);
      }
      //回退到有分叉的地方
      stack.pop();
      length -= currentNode.getTime();
      currentNode = stack.peek();
      while (!(choiceMap.get(currentNode.getStage()) < currentNode.getDestination().size() - 1)) {
        choiceMap.remove(currentNode.getStage());

        stack.pop();
        length -= currentNode.getTime();
        if ( !stack.empty() ) {
          currentNode = stack.peek();
        } else {
          break;
        }
      }
    }
    return maxNodes;
  }

  /**
   * 从trace中生成此次请求的调用关系图
   *
   * @param list 此时间窗口中此次请求所有的trace
   * @return 结果 null表示此request数据不完整
   */
  private List<Node> generateGraph(String requestId, List<Trace> list) {
    //1.从Trace到Node 按照callId分类
    Map<String, Node> map = new HashMap<>();
    Map<String, List<Trace>> callIdMap = new HashMap<>();
    map.put(CLIENT_NAME, Node.clientNode);
    //1.1生成node 只有key
    for (Trace trace : list) {
      String key = trace.getStage();
      if ( !map.containsKey(key) ) {
        map.put(key, Node.generateFromTrace(trace));
      }

      String callId = trace.getCallId();
      if ( !callIdMap.containsKey(callId) ) {
        callIdMap.put(callId, new ArrayList<>());
      }
      callIdMap.get(callId).add(trace);
    }
    //删除客户端的
    callIdMap.remove(null);
    //1.2填满source、destination 以及earlyErrorTrace
    Trace earlyErrorTrace = null;
    for (Trace trace : list) {
      Node node = map.get(trace.getStage());
      //从状态码判断是不是error 若是 记录下发生时间最早的trace
      if ( StatusCode.isError(trace.getStatusCode()) ) {
        if ( earlyErrorTrace != null ) {
          if ( trace.getTimestamp().compareTo(earlyErrorTrace.getTimestamp()) < 0 ) {
            earlyErrorTrace = trace;
          }
        } else {
          earlyErrorTrace = trace;
        }
      } else {
        switch (trace.getEventType()) {
          case AGGREGATE:
            node.setType(Node.Type.AGGREGATE);
          case IN:
            Node sourceNode = map.get(getSourceKey(trace));
            node.addSource(sourceNode);
            node.setStartTime(Long.valueOf(trace.getTimestamp()));
            //为他的前一节点添加des
            if ( sourceNode != clientNode ) {
              if ( sourceNode != null ){
                sourceNode.addDestination(node);
              }else {
                markMiss(requestId);
                return null;
              }
            }
            break;
          case OUT:
            if ( CLIENT_NAME.equals(trace.getRelatedStage()) ) {
              node.addDestination(clientNode);
            }
          case STOP:
            node.setEndTime(Long.valueOf(trace.getTimestamp()));
            break;
          default:
        }
      }
    }
    //1.3找出错误的serviceName 从最后一个开始找 直到找到调用关系的最里面 todo 多线程待测试
    if ( earlyErrorTrace != null ){
      markError(earlyErrorTrace.getRelatedStage(),earlyErrorTrace.getUrl());
      return null;
    }
    else {
      //1.4 没错再  检查完整性并填满time 且 获取client外的第一个Node
      Node firstNode = null;
      for (Node node : map.values()) {
        if ( !node.checkMiss() ) {
          markMiss(requestId);
          return null;
        }
        if ( node != clientNode && node.getSource().get(0) == Node.clientNode ) {
          firstNode = node;
        }
      }
    }

    //2.生成网络Node 通过callId
    for (List<Trace> callIdList : callIdMap.values()) {
      Set<Trace> downSet = new HashSet<>();
      for (Trace trace : callIdList) {
        if ( !downSet.contains(trace) ) {
          downSet.add(trace);
          Node netNode = Node.generateNetworkNode();
          EventType type = trace.getEventType();
          switch (type) {
            case OUT:
            case STOP:
              for (Trace anotherTrace : callIdList) {
                if ( !downSet.contains(anotherTrace) ) {
                  if ( (anotherTrace.getEventType() == EventType.IN ||
                          anotherTrace.getEventType() == EventType.AGGREGATE) &&
                          anotherTrace.getRelatedStage().equals(trace.getStage()) ) {
                    downSet.add(anotherTrace);
                    Node sourceNode = map.get(trace.getStage());
                    Node desNode = map.get(anotherTrace.getStage());
                    netNode.setStartTime(sourceNode.getEndTime());
                    netNode.setEndTime(desNode.getStartTime());
                    netNode.setTime(netNode.getEndTime() - netNode.getStartTime());
                    netNode.addSource(sourceNode);
                    netNode.addDestination(desNode);
                    sourceNode.addDestination(netNode);
                    sourceNode.deleteDestination(desNode);
                    desNode.addSource(netNode);
                    desNode.deleteSource(sourceNode);
                    netNode.setStage(UUID.randomUUID().toString());
                    map.put(netNode.getStage(), netNode);
                    break;
                  }
                }
              }
              break;
            case IN:
            case AGGREGATE:
              for (Trace anotherTrace : callIdList) {
                if ( !downSet.contains(anotherTrace) ) {
                  if ( (anotherTrace.getEventType() == EventType.OUT ||
                          anotherTrace.getEventType() == EventType.STOP) &&
                          trace.getRelatedStage().equals(anotherTrace.getStage()) ) {
                    downSet.add(anotherTrace);
                    netNode.setStage(UUID.randomUUID().toString());
                    Node sourceNode = map.get(anotherTrace.getStage());
                    Node desNode = map.get(trace.getStage());
                    netNode.setStartTime(sourceNode.getEndTime());
                    netNode.setEndTime(desNode.getStartTime());
                    netNode.setTime(netNode.getEndTime() - netNode.getStartTime());
                    netNode.addSource(sourceNode);
                    netNode.addDestination(desNode);
                    sourceNode.addDestination(netNode);
                    sourceNode.deleteDestination(desNode);
                    desNode.addSource(netNode);
                    desNode.deleteSource(sourceNode);
                    map.put(netNode.getStage(), netNode);
                    break;
                  }
                }
              }
              break;
            default:
              break;
          }
        }
      }
    }
    return new ArrayList<>(map.values());
  }

  private void markMiss(String requestId) {
    LOG.info("found miss request,requestId is " + requestId);
    //todo deal with miss
  }

  private void markError(String serviceName,String url) {
    LOG.info("found error service,serviceName is " + serviceName);
    if ( !urlErrorMap.containsKey(serviceName) ){
      Map<String,Long> errorMap = new HashMap<>();
      errorMap.put(url,1L);
      urlErrorMap.put(serviceName,errorMap);
    }else {
      urlErrorMap.get(serviceName).put(url,urlErrorMap.get(serviceName).get(url) + 1 );
    }
  }

  private String getSourceKey(Trace trace) {
    if ( CLIENT_NAME.equals(trace.getRelatedStage()) ) {
      return CLIENT_NAME;
    }
    return trace.getRelatedStage();
  }
}
