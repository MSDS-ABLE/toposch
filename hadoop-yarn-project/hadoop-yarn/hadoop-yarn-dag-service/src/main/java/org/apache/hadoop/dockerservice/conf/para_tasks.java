package org.apache.hadoop.dockerservice.conf;

import java.util.List;

/**
 * Created by root on 8/1/17.
 */
public class para_tasks {
    private int num_tasks;
    private List<Task_service> paras;

    public para_tasks(List<Task_service> tasks, int nums){
        this.paras = tasks;
        num_tasks = nums;
    }

    public int getNum_tasks() {
        return num_tasks;
    }

    public void setNum_tasks(int num_tasks) {
        this.num_tasks = num_tasks;
    }

    public List<Task_service> getParas() {
        return paras;
    }

    public void setParas(List<Task_service> paras) {
        this.paras = paras;
    }
}
