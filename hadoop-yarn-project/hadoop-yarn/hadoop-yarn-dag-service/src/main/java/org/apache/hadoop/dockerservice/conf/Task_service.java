package org.apache.hadoop.dockerservice.conf;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.dockerservice.drive.service.DSConstants;

/**
 * Created by ZJY on 7/31/17.
 */
public abstract class Task_service {
    private String desc;//task description
    private String requirecpu;
    private String requiremem;
    private String cname;
    private String imagesname;
    private String port;
    public  String executor_command;

    public String getCname() {
        return cname;
    }

    public void setCname(String cname) {
        this.cname = cname;
    }

    private Log LOG = LogFactory.getLog(Task_service.class);

    public Task_service(){
        desc = "";
        requirecpu = "1";
        requiremem = "100";
        cname = this.getClass().getName();
        this.imagesname = DSConstants.DOCKERIMAGENAME;
        this.port = DSConstants.SERVICEPORT;
    }
    public Task_service(String desc,  String cpu, String mem, String cname){
        this.requirecpu = cpu;
        this.requiremem = mem;
        this.desc = desc;
        this.cname = cname;
    }

    public Task_service(String desc, String cpu, String mem, String cname, String imagesname, String port){
        this.requirecpu = cpu;
        this.requiremem = mem;
        this.desc = desc;
        this.cname = cname;
        this.imagesname = imagesname;
        this.port = port;
    }


    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getRequirecpu() {
        return requirecpu;
    }

    public void setRequirecpu(String requirecpu) {
        this.requirecpu = requirecpu;
    }

    public String getRequiremem() {
        return requiremem;
    }

    public void setRequiremem(String requiremem) {
        this.requiremem = requiremem;
    }

    public String toString()
    {
        String Cname = this.getClass().getName();
        String description = "Task " + Cname + ": task is " + desc + " requirecpu is " + requirecpu
                + " requiremem is " + requiremem + ": images is " + imagesname + ": port is " + port;
        return  description;
    }

    public String getImagesname() {
        return imagesname;
    }

    public void setImagesname(String imagesname) {
        this.imagesname = imagesname;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    //Added by XMQ, to set and get the value of the executor_command
    public void setExecutor_command(String executor_command) { this.executor_command = executor_command;}

    public String getExecutor_command() { return executor_command; }
    /**
     *  service which need some user-custome option
     * */
    public abstract String execute() ;
}
