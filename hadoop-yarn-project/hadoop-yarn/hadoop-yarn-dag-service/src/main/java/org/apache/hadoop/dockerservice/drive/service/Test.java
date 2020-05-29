package org.apache.hadoop.dockerservice.drive.service;

/**
 * Created by root on 7/31/17.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dockerservice.conf.Configuration_DAG;
import org.apache.hadoop.dockerservice.conf.Task_service;
import org.apache.hadoop.dockerservice.conf.para_tasks;

import java.util.ArrayList;
import java.util.List;

public class Test {
    private static final Log LOG = LogFactory.getLog(Test.class);

    public static void main(String args[]){
        LOG.info("Test start");
        Configuration_DAG conf = new Configuration_DAG();
        conf.conf_parse();
        List<para_tasks> paras = new ArrayList<para_tasks>();
        paras = conf.getParas();
        int totalnum = 0;
        for (para_tasks para : paras){
            totalnum += para.getNum_tasks();
            for (Task_service task : para.getParas()){
                LOG.info("task name is " + task.getClass().getName());
            }
        }

        LOG.info("totoal contaniers to request is " + totalnum);
    }
}
