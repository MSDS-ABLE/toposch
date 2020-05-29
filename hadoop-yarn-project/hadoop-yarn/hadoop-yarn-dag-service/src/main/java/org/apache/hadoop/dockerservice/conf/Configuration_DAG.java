package org.apache.hadoop.dockerservice.conf;

/**
 * Created by root on 7/30/17.
 */
import javax.xml.parsers.*;

import org.apache.hadoop.dockerservice.drive.service.DSConstants;
import org.w3c.dom.*;

import java.util.ArrayList;
import java.util.List;
import java.io.File;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.junit.Test;

/**
 * Created by ZJY on 7/20/17.
 */
public class Configuration_DAG {

    private static final Log LOG = LogFactory.getLog(Configuration_DAG.class);
    //private static List<Task_service> tasks;//All the task load from conf

	private static List<para_tasks> paras = new ArrayList<para_tasks>();

	public static List<para_tasks> getParas() {
		return paras;
	}

	public static void setParas(List<para_tasks> paras) {
		Configuration_DAG.paras = paras;
	}

	public static void conf_parse(){
        try{
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            /*File self_file = new File(System.getProperty("user.dir"));
            //String path = self_file.getAbsolutePath();
            LOG.info("path is " + path);*/
			//Get the root node
            Document document = db.parse(new File(DSConstants.DAGCONFPATH));
			LOG.info("############XMQ LOG#############"
					+" DAG configure path " + DSConstants.DAGCONFPATH);
            NodeList Flow_list = document.getElementsByTagName("FLOW");
            LOG.info("Flow length: " + Flow_list.getLength());
		
			for (int i = 0; i < Flow_list.getLength(); ++i)
			{

				Element FlowNode = (Element) Flow_list.item(i);
				short nodeType = FlowNode.getNodeType();

				NodeList tasklist = FlowNode.getElementsByTagName("Task");

				List<Task_service> tmp_list = new ArrayList<Task_service>();
				for(int j = 0; j < tasklist.getLength(); j++){
					//Task_service task = new Task_service();
					Element task_element = (Element) tasklist.item(j);
					String cname = task_element.getElementsByTagName("Taskclass").item(0)
							.getFirstChild().getNodeValue();
                    Task_service task = (Task_service)Class.forName(cname).newInstance();
					String desc = task_element.getElementsByTagName("description").item(0)
							.getFirstChild().getNodeValue();

					String cpu = task_element.getElementsByTagName("requirecpu").item(0)
							.getFirstChild().getNodeValue();

					String mem = task_element.getElementsByTagName("requiremem").item(0)
							.getFirstChild().getNodeValue();
                    String image_name="0";
					String port ="0";
					//Added by XMQ: executor_command
					String executor_command = null;

					if (task_element.getElementsByTagName("image-name").getLength() != 0 ){
						image_name =  task_element.getElementsByTagName("image-name").item(0).getFirstChild().getNodeValue();
					}else{
						image_name = DSConstants.DOCKERIMAGENAME;
					}
					if (task_element.getElementsByTagName("service-port").getLength() != 0){
						port = task_element.getElementsByTagName("service-port").item(0).getFirstChild().getNodeValue();
					}else{
						port = DSConstants.SERVICEPORT;
					}
					//Added by XMQ: get the value of executor_command from tag
					if (task_element.getElementsByTagName("execute_command").getLength() != 0){
						executor_command = task_element.getElementsByTagName("execute_command").item(0).getFirstChild().getNodeValue();
					} else {
						executor_command = "echo " + cname + " && while true; do sleep 1;done";
					}
					LOG.info("########XMQ LOG########" + "Tag executor_command is " + executor_command);

					task.setRequiremem(mem);
					task.setRequirecpu(cpu);
					task.setDesc(desc);
					task.setCname(cname);
					task.setImagesname(image_name);
					task.setPort(port);
					//Added by XMQ: executor_command
					task.setExecutor_command(executor_command);
                    /*LOG.info("cname is " + cname +" desc is " + desc
							+ " cpu is " + cpu + " mem is " + mem);
					//LOG.info(task.toString());*/
                    LOG.info("###ZJY " + task.toString());
					tmp_list.add(task);//add to all list
				}

				paras.add(new para_tasks(tmp_list,tmp_list.size()));

			}
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    @Test
	public void Test_conf(){
    	conf_parse();
	}
}
