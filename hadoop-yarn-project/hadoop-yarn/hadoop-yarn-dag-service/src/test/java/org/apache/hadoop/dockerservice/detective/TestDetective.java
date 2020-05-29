package org.apache.hadoop.dockerservice.detective;

import com.alibaba.fastjson.JSON;
import com.gaea.common.Trace;
import org.apache.hadoop.dockerservice.detective.graph.Node;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;


public class TestDetective {

  private Log LOG = LogFactory.getLog(TestDetective.class);
  private static final String TRACE_KEY_NAME = "trace";
  private static final String HOST = "192.168.1.191";

  @Test
  public void testLPOP() {
    Jedis jedis = new Jedis(HOST);
    Long length = jedis.llen(TRACE_KEY_NAME);
    LOG.info("读取到" + length + "条数据");
    List<Trace> list = new ArrayList<>();
    for (long i = 0; i < length; i++) {
      list.add(JSON.parseObject(jedis.lpop(TRACE_KEY_NAME), Trace.class));
    }

    LOG.info(list);
  }

  @Test
  public void testLog() {
    LOG.info("hello spring festival");
  }

  @Test
  public void testDetective() {
    Detective detective = new Detective();
    detective.compute();
  }

  @Test
  public void testFindCP(){
    Detective detective =new Detective();
    List<Node> nodes = new ArrayList<>();
    Node node1 = new Node("111",1);
    Node node2 = new Node("222",3);
    Node node3 = new Node("333",5);
    Node node4 = new Node("444",4);
    Node node5 = new Node("555",8);
    Node node6 = new Node("666",7);
    Node node7 = new Node("777",3);
    Node node8 = new Node("888",6);
    Node node9 = new Node("999",4);

    node1.addSource(Node.clientNode);
    node1.addDestination(node2);
    node1.addDestination(node4);

    node2.addSource(node1);
    node2.addDestination(node3);

    node3.addSource(node2);
    node3.addDestination(node7);

    node4.addSource(node1);
    node4.addDestination(node5);
    node4.addDestination(node6);

    node5.addSource(node4);
    node5.addDestination(node7);

    node6.addSource(node4);
    node6.addDestination(node8);

    node7.addSource(node3);
    node7.addSource(node5);
    node7.addDestination(node9);

    node8.addSource(node6);
    node8.addDestination(node9);

    node9.addSource(node7);
    node9.addSource(node8);
    node9.addDestination(Node.clientNode);

    nodes.add(node1);
    nodes.add(node2);
    nodes.add(node3);
    nodes.add(node4);
    nodes.add(node5);
    nodes.add(node6);
    nodes.add(node7);
    nodes.add(node8);
    nodes.add(node9);

    detective.findCP(nodes);

  }
}
