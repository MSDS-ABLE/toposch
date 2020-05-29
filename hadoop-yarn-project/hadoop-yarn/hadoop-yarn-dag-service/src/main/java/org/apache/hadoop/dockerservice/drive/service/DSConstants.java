package org.apache.hadoop.dockerservice.drive.service;

import java.util.HashMap;
/**
 * Constants used in both Client and Application Master
 */
public class DSConstants {

  /**
   * Environment key name pointing to the shell script's location
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLOCATION = "DISTRIBUTEDSHELLSCRIPTLOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTTIMESTAMP = "DISTRIBUTEDSHELLSCRIPTTIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DISTRIBUTEDSHELLSCRIPTLEN = "DISTRIBUTEDSHELLSCRIPTLEN";

  /**
   * Environment key name denoting the timeline domain ID.
   */
  public static final String  DISTRIBUTEDSHELLTIMELINEDOMAIN = "DISTRIBUTEDSHELLTIMELINEDOMAIN";

  //ADD by ZJY, absolute path of DAG_SERVICE
  public static final String DAG_CONFPATH = "/home/ivic/hadoop-3.0.0-beta1/etc/hadoop/DAG_SERVICE.xml";

  //Added by XMQ, absolute path of DAG_SERVICES
  public static final String DAGCONFPATH = System.getenv("HADOOP_HOME")+"/etc/hadoop/DAG_SERVICE.xml";

  public static final String DOCKERIMAGENAME = "docker-beta1";

  public static final String SERVICEPORT = Integer.toString(0);

  //Added by XMQ 2018.3.23: the value of service environments
  //public static final String CONFIG_SERVICE_PASSWORD = System.getenv("CONFIG_SERVICE_PASSWORD");
  public static final String CONFIG_SERVICE_PASSWORD = "root";
  public static final String NOTIFICATION_SERVICE_PASSWORD = "root";
  public static final String STATISTICS_SERVICE_PASSWORD = "root";
  public static final String ACCOUNT_SERVICE_PASSWORD ="root";
  public static final String MONGODB_PASSWORD = "root";
  public static final String REDIS_IP = System.getenv().get("HOST_IP");
  //added by junqing xiao 18.4.9
  public static final String PORT_SYN_TRUE = "true";
  public static final String PORT_SYN_FALSE = "false";

  public static final HashMap<String , String> HostMap= new HashMap<String , String>(){{
    put("super01.ivic.org.cn","192.168.7.70");
    put("super02.ivic.org.cn","192.168.7.71");
    put("super03.ivic.org.cn","192.168.7.72");
    put("super04.ivic.org.cn","192.168.7.73");
    put("super05.ivic.org.cn","192.168.7.74");
    put("super06.ivic.org.cn","192.168.7.75");
    put("super07.ivic.org.cn","192.168.7.76");
    put("super08.ivic.org.cn","192.168.7.77");
    put("super09.ivic.org.cn","192.168.7.78");
    put("super10.ivic.org.cn","192.168.7.79");
    put("super11.ivic.org.cn","192.168.7.80");
    put("super12.ivic.org.cn","192.168.7.81");
    //added by junqing xiao 18.4.23
    put("super01","192.168.7.70");
    put("super02","192.168.7.71");
    put("super03","192.168.7.72");
    put("super04","192.168.7.73");
    put("super05","192.168.7.74");
    put("super06","192.168.7.75");
    put("super07","192.168.7.76");
    put("super08","192.168.7.77");
    put("super09","192.168.7.78");
    put("super10","192.168.7.79");
    put("super11","192.168.7.80");
    put("super12","192.168.7.81");
  }};

}
