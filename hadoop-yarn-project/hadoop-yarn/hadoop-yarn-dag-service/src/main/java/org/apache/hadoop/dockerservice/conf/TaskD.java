package org.apache.hadoop.dockerservice.conf;

/**
 * Created by root on 8/1/17.
 */
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
public class TaskD extends Task_service {
    private static Log LOG = LogFactory.getLog(TaskD.class);
    public TaskD(){
        super();
    }

    public void executor_command(){
        String option = "echo " + getCname() + " && " + "service mysql start";
        String loop = " && while true;do sleep 1;done";
        this.executor_command = option + loop;
    }
    @Override
    public String execute() {
        return this.executor_command;
    }
}

