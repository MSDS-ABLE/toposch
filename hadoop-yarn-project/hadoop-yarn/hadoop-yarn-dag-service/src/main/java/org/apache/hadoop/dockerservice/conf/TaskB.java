package org.apache.hadoop.dockerservice.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by root on 8/1/17.
 */
public class TaskB extends Task_service {
    Log LOG = LogFactory.getLog(TaskB.class);
    public TaskB(){
        super();
    }


    public void executor_command(){
        String option = "echo " + getCname() + " && " + "service mysql start";
        String loop = " && while true;do sleep 1;done";
        this.executor_command = option + loop;
    }
    @Override
    public String execute() {
        executor_command();
        return this.executor_command;
    }
}
