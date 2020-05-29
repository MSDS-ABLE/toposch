package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerScore;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BatchAvoid in RM
 * created by Junqing Xiao 18.3.25
 */

public class BatchAvoidManager extends AbstractService {
  private static final Log LOG = LogFactory.getLog(BatchAvoidManager.class);

  public static final double AVOID_FACTOR = 0.8;
  public static final String FILE_NAME = "enable-batch-avoid.conf";
//  public static final double NODE_LOCAL_FACTOR = 1;
//  public static final double RACK_LOCAL_FACTOR = 0.8;
//  public static final double OFF_SWITCH_FACTOR = 0.6;

  private AtomicBoolean enableBatchAvoid = new AtomicBoolean(false);

  /**
   * 结构是<NodeId,<ContainerId,Score>>
   */
  private ConcurrentHashMap<NodeId,ConcurrentHashMap<ContainerId,Double>> nodeViewMap = new ConcurrentHashMap<>();

  private RMContext rmContext;

  /**
   * Construct the service.
   */
  public BatchAvoidManager(RMContext rmContext) {
    super(BatchAvoidManager.class.getName());
    this.rmContext = rmContext;
  }


  public void updateContainerScore(ContainerScore containerScore){
    ConcurrentMap<NodeId, RMNode> rmNodes = rmContext.getRMNodes();
    ContainerId id = containerScore.getId();
    Double score = containerScore.getScore();
    //todo 如何保证Container退出的时候 这里维护的Container信息都抹掉 可以在这里做
    for (RMNode node : rmNodes.values()){
      NodeId nodeId  = node.getNodeID();
      if ( node.getOnlineContainers().contains(id) ){
        if ( nodeViewMap.get(node.getNodeID()) == null ){
          ConcurrentHashMap<ContainerId,Double> map = new ConcurrentHashMap<>();
          map.put(id,score);
          nodeViewMap.put(nodeId,map);
        }else {
          nodeViewMap.get(node.getNodeID()).put(id,score);
        }
        break;
      }
    }
  }

  public boolean getEnableFlag(){
    return enableBatchAvoid.get();
  }

  //根据不同的数据本地性类型算不同的分
  public double getNodeScore(NodeId nodeId, NodeType type){
    double factor = 1;
//    switch (type){
//      case NODE_LOCAL:
//        factor = NODE_LOCAL_FACTOR;
//        break;
//      case RACK_LOCAL:
//        factor = RACK_LOCAL_FACTOR;
//        break;
//      case OFF_SWITCH:
//        factor = OFF_SWITCH_FACTOR;
//        break;
//      default:
//          break;
//    }
    if ( !enableBatchAvoid.get() || getNodeScores().get(nodeId) == null ){
      return 0;
    }else {
      return getNodeScores().get(nodeId) * factor;
    }
  }

  /*
  private Map<NodeId,Double> getNodeScores(){
    //nodeViewMap ==> nodeScores;
    Map<NodeId,Double> nodeScores = new HashMap<>();
    double max = 0;
    for (Map.Entry<NodeId,ConcurrentHashMap<ContainerId,Double>> entry : nodeViewMap.entrySet()){
      NodeId nodeId = entry.getKey();
      double score = 0;
      for (Map.Entry<ContainerId,Double> containerEntry : entry.getValue().entrySet()){
        score += containerEntry.getValue();
      }
      nodeScores.put(nodeId,score);
      if ( score > max ){
        max = score;
      }
    }


    //归一化 "一"为AVOID_FACTOR 用户指定
    Map<NodeId,Double> returnMap = new HashMap<>();
    for (Map.Entry<NodeId,Double> entry : nodeScores.entrySet()){
      returnMap.put(entry.getKey(),entry.getValue() / max * AVOID_FACTOR);
    }

    StringBuilder builder = new StringBuilder("**nodeScores**  {");
    for (Map.Entry<NodeId, Double> entry1 : returnMap.entrySet()) {
      builder.append("[").append(entry1.getKey())
              .append("::").append(entry1.getValue()).append("]");
    }
    builder.append("}");
    LOG.info(builder.toString());
    return returnMap;
  }
  */

  private Map<NodeId,Double> getNodeScores(){
    //nodeViewMap ==> nodeScores;
    Map<NodeId,Double> nodeScores = new HashMap<>();
    double sum = 0;
    double mean = 0;
    StringBuilder bd = new StringBuilder("**nodeViewMap**  {");
    for (Map.Entry<NodeId,ConcurrentHashMap<ContainerId,Double>> entry : nodeViewMap.entrySet()){
      NodeId nodeId = entry.getKey();
      double score = 0;
      for (Map.Entry<ContainerId,Double> containerEntry : entry.getValue().entrySet()){
        score += containerEntry.getValue();
      }
      nodeScores.put(nodeId,score);
      sum += score;
      bd.append("[").append(nodeId)
              .append("::").append(score).append("]");
    }
    bd.append("}");
    LOG.info(bd.toString());
    mean = sum / nodeScores.size();
    double sum2 = 0;
    for (Map.Entry<NodeId,Double> entry : nodeScores.entrySet()){
      sum2 += (entry.getValue() - mean) * (entry.getValue() - mean);
    }
    double std = Math.sqrt(sum2);

    //归一化 z-score
    Map<NodeId,Double> returnMap = new HashMap<>();
    for (Map.Entry<NodeId,Double> entry : nodeScores.entrySet()){
      double v = ((entry.getValue() - mean ) / std) / 2 + 0.5;
      if ( v < 0.5 * AVOID_FACTOR ){
        if ( v < 0.3 * AVOID_FACTOR ){
          returnMap.put(entry.getKey(),0.0);
        }else {
          if ( Double.isNaN((v - 0.3 * AVOID_FACTOR) * AVOID_FACTOR) ){
            returnMap.put(entry.getKey(),0.0);
          }else {
            returnMap.put(entry.getKey(),(v - 0.3 * AVOID_FACTOR) * AVOID_FACTOR);
          }
        }
      }else {
        if ( Double.isNaN(v * AVOID_FACTOR)){
          returnMap.put(entry.getKey(),0.0);
        }else {
          returnMap.put(entry.getKey(),v * AVOID_FACTOR);
        }
      }
    }

    StringBuilder builder = new StringBuilder("**nodeScores**  {");
    for (Map.Entry<NodeId, Double> entry1 : returnMap.entrySet()) {
      builder.append("[").append(entry1.getKey())
              .append("::").append(entry1.getValue()).append("]");
    }
    builder.append("}");
    LOG.info(builder.toString());
    return returnMap;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    //added by junqing xiao 18.6.5
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          String encoding = "UTF-8";
          File file = new File(System.getenv().get("HADOOP_CONF_DIR") +
                  "/" + FILE_NAME);
          if (file.isFile() && file.exists()) {
            InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            if ((lineTxt = bufferedReader.readLine()) != null) {
              //这个jdk的转换是 true --> true 其他比如yes --> false
              boolean flag = Boolean.valueOf(lineTxt);
              enableBatchAvoid.set(flag);
              if ( !flag ){
                nodeViewMap = new ConcurrentHashMap<>();
              }
              LOG.info("read " + FILE_NAME + ", flag is  " + flag);
            }
            read.close();
          } else {
            LOG.warn("can not find " + FILE_NAME );
          }
        } catch (Exception e) {
          LOG.error(e);
        }

        if ( enableBatchAvoid.get() ){
          Map<NodeId,Double> returnMap = getNodeScores();
          for (Map.Entry<NodeId,Double> entry : returnMap.entrySet()){
            LOG.info("&DataNodeScoreID=" + entry.getKey() + "&DataNodeScore=" + entry.getValue());
          }
        }else {
          for (Map.Entry<NodeId,ConcurrentHashMap<ContainerId,Double>> entry : nodeViewMap.entrySet()){
            NodeId nodeId = entry.getKey();
            double score = 0;
            for (Map.Entry<ContainerId,Double> containerEntry : entry.getValue().entrySet()){
              score += containerEntry.getValue();
            }
            LOG.info("&DataNodeScoreID=" + nodeId + "&DataNodeScore=0");
          }
        }
      }
    };
    ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    // 参数：1、任务体 2、首次执行的延时时间 3、任务执行间隔 4、间隔时间单位
    service.scheduleAtFixedRate(runnable, 0, 10, TimeUnit.SECONDS);
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

}
