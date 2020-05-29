/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.dockerservice.drive.service;

import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dockerservice.conf.Configuration_DAG;
import org.apache.hadoop.dockerservice.conf.Task_service;
import org.apache.hadoop.dockerservice.conf.para_tasks;
import org.apache.hadoop.dockerservice.detective.Detective;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.LogManager;
import org.junit.*;

import java.io.*;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 *
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 *
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed. However, in the distributedshell, trackingurl
 * and appMasterHost:appMasterRpcPort are not supported.
 * </p>
 *
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 *
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 *
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 *
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
 * container's {@link ContainerId}.
 *
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);


    public static enum DSEvent {
        DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
    }


    public static enum DSEntity {
        DS_APP_ATTEMPT, DS_CONTAINER
    }

    private static final String YARN_SHELL_ID = "YARN_SHELL_ID";

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync amRMClient;

    // In both secure and non-secure modes, this points to the job-submitter.
    UserGroupInformation appSubmitterUgi;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;

    // Application Attempt Id ( combination of attemptId and fail count )
    protected ApplicationAttemptId appAttemptID;

    // TODO
    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    private boolean timelineServiceV2 = false;

    // App Master configuration
    // No. of containers to run shell command on

    protected static int numTotalContainers = 4;
    // Memory to request for the container on which the shell command will run
    private long containerMemory = 10;
    // VirtualCores to request for the container on which the shell command will run
    private int containerVirtualCores = 1;
    // Priority of the request
    private int requestPriority;

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us

    protected AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    protected AtomicInteger numRequestedContainers = new AtomicInteger();

    // Shell command to be executed
    private String shellCommand = "";
    // Args to be passed to the shell command
    private String shellArgs = "";
    // Env variables to be setup for the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();

    // Location of shell script ( obtained from info set in env )
    // Shell script path in fs
    private String scriptPath = "";
    // Timestamp needed for creating a local resource
    private long shellScriptPathTimestamp = 0;
    // File length needed for local resource
    private long shellScriptPathLen = 0;

    // Container retry options
    private ContainerRetryPolicy containerRetryPolicy =
            ContainerRetryPolicy.NEVER_RETRY;
    private Set<Integer> containerRetryErrorCodes = null;
    private int containerMaxRetries = 0;
    private int containrRetryInterval = 0;

    // Timeline domain ID
    private String domainId = null;

    // Hardcoded path to shell script in launch container's local env
    private static final String EXEC_SHELL_STRING_PATH = Client.SCRIPT_PATH
            + ".sh";
    private static final String EXEC_BAT_SCRIPT_STRING_PATH = Client.SCRIPT_PATH
            + ".bat";
    //Added by XMQ 18.3.25
    private String RABBITMQ_HOST = null;
    private String RABBITMQ_PORT=null;
    private String AUTH_SERVICE_HOST=null;
    private String AUTH_SERVICE_PORT=null;
    private String CONFIG_HOST = null;
    private String CONFIG_PORT = null;
    private String REGISTRY_HOST = null; // need to be given
    private String REGISTRY_PORT =null;

    //added by junqing xiao 18.4.18
    public Map<String,ContainerId> service2CIdMap = new ConcurrentHashMap<>();

    // Hardcoded path to custom log_properties
    private static final String log4jPath = "log4j.properties";

    private static final String shellCommandPath = "shellCommands";
    private static final String shellArgsPath = "shellArgs";

    private volatile boolean done;

    private ByteBuffer allTokens;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();

    // Timeline Client
    TimelineClient timelineClient;

    // Timeline v2 Client
    private TimelineV2Client timelineV2Client;

    static final String CONTAINER_ENTITY_GROUP_ID = "CONTAINERS";
    static final String APPID_TIMELINE_FILTER_NAME = "appId";
    static final String USER_TIMELINE_FILTER_NAME = "user";

    private final String linux_bash_command = "bash";
    private final String windows_command = "cmd /c";

    private int yarnShellIdCounter = 1;

    private int max_cpu_need = 1;// for container fail replan
    private long max_mem_need = 10;
    //cpu-mem and responsed tasks
    private Map<String, String>  task_container = new HashMap<String, String>();
    private static List<task_container_map>  task_maps = new ArrayList<task_container_map>();

    protected final Set<ContainerId> launchedContainers =
            Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

    private  static List<para_tasks> paras = new ArrayList<para_tasks>();
    private Map<ContainerId, Integer> cid_task = new HashMap<ContainerId, Integer>();

    private Map<String, String> task_url_map = new HashMap<String, String>();

    public static void get_paras_new(){
        LOG.info("start get all tasks");
        Configuration_DAG conf = new Configuration_DAG();
        conf.conf_parse();
        paras = conf.getParas();
        numTotalContainers = 0;
        int pre_index = 0;
        int pre_nums = 0;
        int para_nums = 0;
        int post_nums = 0;
        for (int i = 0; i < paras.size(); i++){

            para_tasks para = paras.get(i);
            numTotalContainers += para.getNum_tasks();
            List<Task_service> tasks = para.getParas();
            LOG.info("task_maps size is " + task_maps.size() + " curr flow index is " + i);
            if (i > 0){
                pre_index = task_maps.size() - 1;
                pre_nums = paras.get(i-1).getNum_tasks();
                LOG.info("Flow " + (i+1) + " pretask is " + pre_index);
            }
            para_nums = para.getNum_tasks();

            if (i < paras.size() - 1){
                post_nums = paras.get(i + 1).getNum_tasks();
            }

            for (int j = 0; j < para.getNum_tasks(); j++){
                Task_service task = tasks.get(j);
                String cpu = task.getRequirecpu();
                String mem = task.getRequiremem();
                int multiple = Integer.parseInt(mem)/ 1024;
                int remain = Integer.parseInt(mem) % 1024;
                int real_mem = (remain != 0) ? (multiple + 1) * 1024: multiple * 1024;

                /**
                 * Modefy by ZJY , port and imagename paras should be added
                 */
                task_container_map tmap = new task_container_map(cpu, Integer.toString(real_mem),
                        false, task.getClass().getName(),task.getImagesname(), task.getPort());

                //rewrite by XMQ: replace "task.execute()" with "task.getExecutor_command"
                tmap.set_executor(task.getExecutor_command());
                LOG.debug("########XMQ LOG########"
                        + " Tmap executor command is " + tmap.executor);
                tmap.setPara_nums(para_nums);
                if(pre_index >0 || pre_nums > 0){
                    tmap.set_pre_post_tasks(pre_index, pre_nums, post_nums);
                    LOG.info("### " + tmap.getCname() + " pre_tasks is " + pre_index + " and " + pre_nums);
                }else{
                    tmap.set_pre_post_tasks(pre_index, pre_nums, post_nums);
                    LOG.info("### " + tmap.getCname() + " has no pre tasks");
                }
                task_maps.add(tmap);
                LOG.info("Initing task maps " + tmap.toString());
            }
            pre_index = 0;pre_nums = 0;para_nums = 0;post_nums = 0;
        }
    }
    class synchronized_tasks{
        int pre_nums;
        int pre_index;
        int position;
        int para_nums;
        int post_nums;
        boolean wait_flag = false;
        boolean notify_flag = true;

        public synchronized_tasks(int pre_index, int pre_nums, int position,
                                  int para_nums, int post_nums){
            this.para_nums = para_nums; this.position = position; this.pre_index = pre_index;
            this.pre_nums = pre_nums; this.post_nums = post_nums;
        }
        public synchronized void task_wait(){
            LOG.info("### " + task_maps.get(position).getCname() + " begin to execute");
            //judge the pre_tasks for wait
            if (pre_index == 0){
                LOG.info("this task has no pre task");
            }else{
                for (int i = pre_index; i > pre_index - pre_nums; i--){
                    LOG.info("### this task pre_task is " + task_maps.get(i).getCname());
                }
            }
        }
        public synchronized void task_notify(){
            //judge the para_tasks for notify

        }
        public void tostring(){
            LOG.info("### " + task_maps.get(position).getCname() + " pre_index="+pre_index+
                    ",pre_nums="+pre_nums+",para_nums="+para_nums+",post_nums="+post_nums+",postion="+position);
        }
    }
    private static class task_container_map{
        public String cpu;
        public String mem;
        public boolean is_mapped;
        public boolean is_execute;
        public int pre_index;
        public int pre_nums;
        public String cname;

        //Modified by ZJY
        public String port;
        public String imagename;
        public String executor;

        public String getPort() {
            return port;
        }

        public void setPort(String port) {
            this.port = port;
        }

        public String getImagename() {
            return imagename;
        }

        public void setImagename(String imagename) {
            this.imagename = imagename;
        }


        public boolean is_pre_exec(){
            LOG.debug("##########XMQ LOG############"
                    + " pre_nums is: " + pre_nums
                    + " pre_index is: " + pre_index);
            if (pre_nums == 0){
                LOG.info("### " + cname + " has no pre_task");
                return true;
            }else{
                for (int i = pre_index; i > pre_index - pre_nums; i--){
                    if (task_maps.get(i).is_execute == false){
                        //LOG.info("### pre_task "  + task_maps.get(i).getCname() + " has not been executed");
                        return false;
                    }
                    //LOG.info("### pre_task "  + task_maps.get(i).getCname() + " has been just executed");
                }
            }
            return true;
        }

        public int getPost_nums() {
            return post_nums;
        }

        public void setPost_nums(int post_nums) {
            this.post_nums = post_nums;
        }

        public int post_nums;

        public int getPara_nums() {
            return para_nums;
        }

        public void setPara_nums(int para_nums) {
            this.para_nums = para_nums;
        }

        public int para_nums;
        public List<task_container_map> pre_tasks;
        public task_container_map(){
            this.cpu = "1"; mem = "1024"; is_mapped = false;
            cname = Task_service.class.getName();
        }
        public task_container_map(String cpu, String mem, boolean is_mapped, String cname,String imagename, String port){
            this.cpu = cpu; this.mem = mem; this.is_mapped = false;
            this.cname = cname; this.is_execute = false; this.imagename = imagename; this.port = port;
        }

        public String getCname() {
            return cname;
        }

        //set pre_tasks for the current task
        public void set_pre_post_tasks(int pre_index, int pre_nums, int post_num){
            this.pre_index = pre_index;
            this.pre_nums = pre_nums;
            this.post_nums = post_num;
        }

        public int getPre_index() {
            return pre_index;
        }

        public void setPre_index(int pre_index) {
            this.pre_index = pre_index;
        }

        public int getPre_nums() {
            return pre_nums;
        }

        public void setPre_nums(int pre_nums) {
            this.pre_nums = pre_nums;
        }

        public void setCname(String cname) {
            this.cname = cname;
        }

        public String getCpu() {

            return cpu;
        }

        public void setCpu(String cpu) {
            this.cpu = cpu;
        }

        public String getMem() {
            return mem;
        }

        public void setMem(String mem) {
            this.mem = mem;
        }

        public boolean isIs_mapped() {
            return is_mapped;
        }

        public boolean isIs_execute() {
            return is_execute;
        }

        public void setIs_execute(boolean is_execute) {
            this.is_execute = is_execute;
        }

        public void setIs_mapped(boolean is_mapped) {
            this.is_mapped = is_mapped;
        }

        public void set_executor(String executor){
            this.executor = executor;
        }
        public String get_executor(){
            return this.executor;
        }
        public String toString(){
            return "task_Map: + " + "< " + this.cpu + ","
                    + this.mem + "," + this.is_mapped + "," +  this.cname + "," + this.imagename + "," + this.port + "," + this.executor + ">";
        }
    }
    @org.junit.Test
    public void Test_conf(){
        get_paras_new();
    }
    public static void main(String[] args) {
        boolean result = false;

        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            //get all the dag tasks from Configuration_DAG
            //get_paras();
            get_paras_new();
            if (!doRun) {
                System.exit(0);
            }
            appMaster.run();
            result = appMaster.finish();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     */
    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        BufferedReader buf = null;
        try {
            String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
                    Shell.execCommand("ls", "-al");
            buf = new BufferedReader(new StringReader(lines));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.cleanup(LOG, buf);
        }
    }

    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {
        //added by junqing xiao 18.4.10 start redis here
        Runtime.getRuntime().exec("/usr/local/bin/redis-server");

        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("container_retry_policy", true,
                "Retry policy when container fails to run, "
                        + "0: NEVER_RETRY, 1: RETRY_ON_ALL_ERRORS, "
                        + "2: RETRY_ON_SPECIFIC_ERROR_CODES");
        opts.addOption("container_retry_error_codes", true,
                "When retry policy is set to RETRY_ON_SPECIFIC_ERROR_CODES, error "
                        + "codes is specified with this option, "
                        + "e.g. --container_retry_error_codes 1,2,3");
        opts.addOption("container_max_retries", true,
                "If container could retry, it specifies max retires");
        opts.addOption("container_retry_interval", true,
                "Interval between each retry, unit is milliseconds");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        //Check whether customer log4j.properties file exists
        if (fileExist(log4jPath)) {
            try {
                Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
                        log4jPath);
            } catch (Exception e) {
                LOG.warn("Can not set up custom log4j properties. " + e);
            }
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        Map<String, String> envs = System.getenv();

        LOG.info("envs in appMaster");
        for (Map.Entry<String, String> entry : envs.entrySet()){
            LOG.info(entry.getKey() + "->" + entry.getValue() );
        }


        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ApplicationAttemptId.fromString(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ContainerId.fromString(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        if (!fileExist(shellCommandPath)
                && envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION).isEmpty()) {
            throw new IllegalArgumentException(
                    "No shell command or shell script specified to be executed by application master");
        }

        if (fileExist(shellCommandPath)) {
            shellCommand = readContent(shellCommandPath);
        }

        if (fileExist(shellArgsPath)) {
            shellArgs = readContent(shellArgsPath);
        }

        if (cliParser.hasOption("shell_env")) {
            String shellEnvs[] = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }

        if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
            scriptPath = envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

            if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
                shellScriptPathTimestamp = Long.parseLong(envs
                        .get(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
            }
            if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
                shellScriptPathLen = Long.parseLong(envs
                        .get(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN));
            }
            if (!scriptPath.isEmpty()
                    && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
                LOG.error("Illegal values in env for shell script path" + ", path="
                        + scriptPath + ", len=" + shellScriptPathLen + ", timestamp="
                        + shellScriptPathTimestamp);
                throw new IllegalArgumentException(
                        "Illegal values in env for shell script path");
            }
        }

        if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN)) {
            domainId = envs.get(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN);
        }

        containerMemory = Integer.parseInt(cliParser.getOptionValue(
                "container_memory", "10"));
        containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
                "container_vcores", "1"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
                "num_containers", "4"));
        LOG.info("after Init , total num of containers is " + numTotalContainers);
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run distributed shell with no containers");
        }
        requestPriority = Integer.parseInt(cliParser
                .getOptionValue("priority", "0"));

        containerRetryPolicy = ContainerRetryPolicy.values()[
                Integer.parseInt(cliParser.getOptionValue(
                        "container_retry_policy", "0"))];
        if (cliParser.hasOption("container_retry_error_codes")) {
            containerRetryErrorCodes = new HashSet<>();
            for (String errorCode :
                    cliParser.getOptionValue("container_retry_error_codes").split(",")) {
                containerRetryErrorCodes.add(Integer.parseInt(errorCode));
            }
        }
        containerMaxRetries = Integer.parseInt(
                cliParser.getOptionValue("container_max_retries", "0"));
        containrRetryInterval = Integer.parseInt(cliParser.getOptionValue(
                "container_retry_interval", "0"));

        if (YarnConfiguration.timelineServiceEnabled(conf)) {
            timelineServiceV2 = YarnConfiguration.timelineServiceV2Enabled(conf);
        } else {
            timelineClient = null;
            LOG.warn("Timeline service is not enabled");
        }

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     *///                    Thread launchThread = createLaunchContainerThread(allocatedContainer,
//                            yarnShellId);
    @SuppressWarnings({ "unchecked" })
    public void run() throws YarnException, IOException, InterruptedException {
        LOG.info("Starting ApplicationMaster");

        // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
        // are marked as LimitedPrivate
        Credentials credentials =
                UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

        // Create appSubmitterUgi and add original tokens to it
        String appSubmitterUserName =
                System.getenv(Environment.USER.name());
        appSubmitterUgi =
                UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);

        AMRMClientAsync.AbstractCallbackHandler allocListener =
                new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(5000, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        startTimelineClient(conf);
        if (timelineServiceV2) {
            // need to bind timelineClient
            amRMClient.registerTimelineV2Client(timelineV2Client);
        }
        if(timelineClient != null) {
            if (timelineServiceV2) {
                publishApplicationAttemptEventOnTimelineServiceV2(
                        DSEvent.DS_APP_ATTEMPT_START);
            } else {
                publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                        DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
            }
        }

        // Setup local RPC Server to accept status requests directly from clients
        // TODO need to setup a protocol for client to be able to communicate to
        // the RPC server
        // TODO use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        long maxMem = response.getMaximumResourceCapability().getMemorySize();
        LOG.info("Max mem capability of resources in this cluster " + maxMem);

        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

        // A resource ask cannot exceed the max.
       /* if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerMemory + ", max="
                    + maxMem);
            containerMemory = maxMem;
        }

        if (containerVirtualCores > maxVCores) {
            LOG.info("Container virtual cores specified above max threshold of cluster."
                    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                    + maxVCores);
            containerVirtualCores = maxVCores;
        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        for(Container container: previousAMRunningContainers) {
            launchedContainers.add(container.getId());
        }
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());


        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();
        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).
        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM(containerVirtualCores,containerMemory);
            amRMClient.addContainerRequest(containerAsk);
        }*/
        task_container_request(maxMem, maxVCores, response);
        numRequestedContainers.set(numTotalContainers);

        //added by junqing xiao 18.4.10 start detective thread
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
//                StringBuilder stringBuilder2 = new StringBuilder("junqing xiao : service2CIdMap-->{");
//                for (Map.Entry<String,ContainerId> entry : service2CIdMap.entrySet()){
//                  stringBuilder2.append("<").append(entry.getKey()).append(":").append(entry.getValue()).append(">,");
//                }
//                stringBuilder2.append("}");
//                LOG.info(stringBuilder2.toString());
                List<ContainerScore> list = new ArrayList<>();
                StringBuilder stringBuilder = new StringBuilder("junqing xiao : scoreMap-->{");
                Detective detective = new Detective();
                Map<String,Double> scoreMap = detective.compute();

                for (Map.Entry<String,ContainerId> entry : service2CIdMap.entrySet()){
                    String serviceName = entry.getKey();
                    ContainerId containerId = entry.getValue();
                    double score = scoreMap.getOrDefault(serviceName,0.0);

                    stringBuilder.append("<").append(serviceName).append(":").append(score).append(">,");
                    list.add(ContainerScore.newInstance(containerId,score));
                }
                stringBuilder.append("}");
                LOG.info(stringBuilder.toString());
                amRMClient.setScoreList(list);
            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 参数：1、任务体 2、首次执行的延时时间 3、任务执行间隔 4、间隔时间单位
        service.scheduleAtFixedRate(runnable, 1, 3, TimeUnit.MINUTES);
    }

    void task_container_request(long maxMem, int maxVCores, RegisterApplicationMasterResponse response){
        for (int i = 0; i < task_maps.size(); i++){
            task_container_map t_map = task_maps.get(i);
            int cpu = Integer.parseInt(t_map.getCpu());
            long mem = Long.parseLong(t_map.getMem());
            String cname = t_map.getCname();
            boolean is_mapped = t_map.isIs_mapped();
            if (mem > maxMem) {
                LOG.info("Container memory specified above max threshold of cluster."
                        + " Using max value." + ", specified=" + mem + ", max="
                        + maxMem);
                mem = maxMem;
                task_container_map t = new task_container_map
                        (Integer.toString(cpu), Long.toString(mem ), is_mapped, cname, t_map.getImagename(),t_map.getPort());
                task_maps.set(i, t);
            }
            if (cpu > maxVCores) {
                LOG.info("Container virtual cores specified above max threshold of cluster."
                        + " Using max value." + ", specified=" + containerVirtualCores + ", max="
                        + maxVCores);
                cpu = maxVCores;
                task_container_map t = new task_container_map
                        (Integer.toString(cpu), Long.toString(mem ), is_mapped, cname, t_map.getImagename(),t_map.getPort());
                task_maps.set(i, t);
            }

        }

        List<Container> previousAMRunningContainers =
                response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        for(Container container: previousAMRunningContainers) {
            launchedContainers.add(container.getId());
        }
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());


        int numTotalContainersToRequest =
                numTotalContainers - previousAMRunningContainers.size();
        // Setup ask for containers from RM
        // Send request for containers to RM
        // Until we get our fully allocated quota, we keep on polling RM for
        // containers
        // Keep looping until all the containers are launched and shell script
        // executed on them ( regardless of success/failure).

        /**
         * AM-Orchestration ZJY
         * */
        for (int j = 0; j < numTotalContainersToRequest; ++j) {
            ContainerRequest containerAsk = setupContainerAskForRM(Integer.parseInt(task_maps.get(j).cpu),
                    Long.parseLong(task_maps.get(j).mem),MixType.ONLINE, ServicePort.newInstance(Integer.parseInt(task_maps.get(j).port)));
            LOG.info("AM-Orchestration ZJY : required service send to RM is " + task_maps.get(j).getCname() + ":"+task_maps.get(j).port);
            amRMClient.addContainerRequest(containerAsk);
        }
    }

    void startTimelineClient(final Configuration conf)
            throws YarnException, IOException, InterruptedException {
        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    if (YarnConfiguration.timelineServiceEnabled(conf)) {
                        // Creating the Timeline Client
                        if (timelineServiceV2) {
                            timelineV2Client = TimelineV2Client.createTimelineClient(
                                    appAttemptID.getApplicationId());
                            timelineV2Client.init(conf);
                            timelineV2Client.start();
                            LOG.info("Timeline service V2 client is enabled");
                        } else {
                            timelineClient = TimelineClient.createTimelineClient();
                            LOG.info("Timeline service V1 client is enabled");
                            timelineClient = TimelineClient.createTimelineClient();
                            timelineClient.init(conf);
                            timelineClient.start();
                            LOG.info("Timeline service V1 client is enabled");
                        }
                    } else {
                        timelineClient = null;
                        timelineV2Client = null;
                        LOG.warn("Timeline service is not enabled");
                    }
                    return null;
                }
            });
        } catch (UndeclaredThrowableException e) {
            throw new YarnException(e.getCause());
        }
    }


    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }


    protected boolean finish() {
        // wait for completion.
        while (!done
                && (numCompletedContainers.get() != numTotalContainers)) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {}
        }

        if (timelineClient != null) {
            if (timelineServiceV2) {
                publishApplicationAttemptEventOnTimelineServiceV2(
                        DSEvent.DS_APP_ATTEMPT_END);
            } else {
                publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
                        DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
            }
        }

        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        boolean success = true;
        if (numCompletedContainers.get() - numFailedContainers.get()
                >= numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", allocated="
                    + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            LOG.info(appMessage);
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }

        amRMClient.stop();

        // Stop Timeline Client
        if(timelineClient != null) {
            timelineClient.stop();
        }

        return success;
    }


    class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @SuppressWarnings("unchecked")
        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info(appAttemptID + " got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());
                /*
                * recording the task which has been executed success and recudler the task behind it
                * */
                int index = cid_task.get(containerStatus.getContainerId());
                task_container_map tmap = task_maps.get(index);
                if (tmap != null){
                    tmap.is_execute = true;
                }
                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);
                // ignore containers we know nothing about - probably from a previous
                // attempt
                if (!launchedContainers.contains(containerStatus.getContainerId())) {
                    LOG.info("Ignoring completed status of "
                            + containerStatus.getContainerId()
                            + "; unknown container(probably launched by previous attempt)");
                    continue;
                }

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    LOG.info("### 0 != exitStatus");
                    // container failed
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId="
                            + containerStatus.getContainerId());
                }
                if(timelineClient != null) {
                    if (timelineServiceV2) {
                        publishContainerEndEventOnTimelineServiceV2(containerStatus);
                    } else {
                        publishContainerEndEvent(
                                timelineClient, containerStatus, domainId, appSubmitterUgi);
                    }
                }
            }

            // ask for more containers if any failed
            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);
            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM(max_cpu_need, max_mem_need,MixType.ONLINE,ServicePort.newInstance(0));
                    amRMClient.addContainerRequest(containerAsk);
                }
            }
            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
            LOG.info("###Compeleted message is:" + done);
        }

        /**
         *callback function when get the allocated contianers,
         * */
        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                String yarnShellId = Integer.toString(yarnShellIdCounter);
                yarnShellIdCounter++;
                LOG.info("*junqing xiao*:Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", yarnShellId=" + yarnShellId
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                        + ":" + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemorySize()
                        + ", containerResourceVirtualCores"
                        + allocatedContainer.getResource().getVirtualCores()
                        + ", mixType"
                        + allocatedContainer.getMixType());


                String vcore = Integer.toString(allocatedContainer.getResource().getVirtualCores());
                String mem = Long.toString(allocatedContainer.getResource().getMemorySize());
                int index = find_match_and_execute(vcore,mem);
                cid_task.put(allocatedContainer.getId(), index);
                LOG.info("###:Allocated Container: core=" + vcore + "  mem=" +mem);
                if(index == -1) {
                    LOG.error("###:Can not find the container-matched task!");
                    return;
                }
                try {
                    Task_service task = (Task_service)Class.forName(task_maps.get(index).getCname()).newInstance();
                    LOG.info("###ZJY:The container-matched task:" + task.getCname());
                    /**
                     * add the coatainer URL for debug, and the result for each can be recording
                     * */
                    task_url_map.put(task.getCname(), allocatedContainer.getNodeId().getHost());

                    Thread launchThread = createLaunchContainerThread(allocatedContainer,
                            yarnShellId,index);

                    // launch and start the container on a separate thread to keep
                    // the main thread unblocked
                    // as all containers may not be allocated at one go.
                    launchThreads.add(launchThread);
                    launchedContainers.add(allocatedContainer.getId());
                    launchThread.start();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onContainersUpdated(List<UpdatedContainer> containers) {
            //to avoid error
        }

        public int find_match_and_execute(String vcore, String mem){
            LOG.info("#### Matching is on");
            int task_index = -1;
            for(int i= 0 ; i < task_maps.size(); i++)
            {
//                if(Integer.parseInt(vcore) >= Integer.parseInt(task_maps.get(i).cpu) &&
//                        Integer.parseInt(mem) >= Integer.parseInt(task_maps.get(i).mem )&& !task_maps.get(i).is_mapped)
                if(vcore.equals(task_maps.get(i).cpu) && mem.equals(task_maps.get(i).mem )&& !task_maps.get(i).is_mapped)
                {
                    task_index = i;
                    task_maps.get(i).setIs_mapped(true);
                    break;
                }
            }
            return task_index;
        }

        @Override
        public void onShutdownRequest() {
            done = true;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {}

        @Override
        public float getProgress() {
            // set progress to deliver to RM on next heartbeat
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        @Override
        public void onError(Throwable e) {
            LOG.error("Error in RMCallbackHandler: ", e);
            done = true;
            amRMClient.stop();
        }
    }


    static class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers =
                new ConcurrentHashMap<ContainerId, Container>();
        private final ApplicationMaster applicationMaster;

        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }

            Container container = containers.get(containerId);
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            ServicePort port = applicationMaster.nmClientAsync.GetServiceportByContainerIDAsyn(containerId, container.getNodeId());
            //Added by XMQ 2018.3.26: get host and port of config
            int index = applicationMaster.cid_task.get(containerId);
            task_container_map tmap = task_maps.get(index);
            applicationMaster.service2CIdMap.put(tmap.getImagename(),containerId);
            switch (tmap.getImagename()){
                case "rabbitmq:3-management" :
                    applicationMaster.RABBITMQ_HOST = container.getNodeId().getHost();
                    applicationMaster.RABBITMQ_PORT = Integer.toString(port.getServicePort());
                    LOG.debug("#######XMQ LOG##########"
                            + " Image name is: " + tmap.getImagename()
                            + " RABBITMQ_HOST is: " + applicationMaster.RABBITMQ_HOST
                            + " RABBITMQ_PORT is: " + applicationMaster.RABBITMQ_PORT);
                    break;
                case "config" :
                    applicationMaster.CONFIG_HOST = container.getNodeId().getHost();
                    applicationMaster.CONFIG_PORT = Integer.toString(port.getServicePort());
                    LOG.debug("########XMQ LOG############"
                            + " Image name is: " + tmap.getImagename()
                            + " CONFIG_HOST is: " + applicationMaster.CONFIG_HOST
                            + " CONFIG_PORT is: " + applicationMaster.CONFIG_PORT);
                    break;
                case "registry" :
                    applicationMaster.REGISTRY_HOST = container.getNodeId().getHost();
                    applicationMaster.REGISTRY_PORT = Integer.toString(port.getServicePort());
                    LOG.debug("##########XMQ LOG##########"
                            + " Image name is: " + tmap.getImagename()
                            + " REGISTRY_HOST is: " + applicationMaster.REGISTRY_HOST
                            + " REGISTRY_PORT is: " + applicationMaster.REGISTRY_PORT);
                    break;
                case "auth-service" :
                    applicationMaster.AUTH_SERVICE_HOST = container.getNodeId().getHost();
                    applicationMaster.AUTH_SERVICE_PORT = Integer.toString(port.getServicePort());
                    LOG.debug("##########XMQ LOG##########"
                            + " Image name is: " + tmap.getImagename()
                            + " AUTH_SERVICE_HOST is: " + applicationMaster.AUTH_SERVICE_HOST
                            + " AUTH_SERVICE_PORT is: " + applicationMaster.AUTH_SERVICE_PORT);
                    break;
            }

            //Added by XMQ 2018.3.25: set the pre_task be executed
            if (tmap != null){
                tmap.is_execute = true;
            }
            LOG.debug("#########XMQ LOG###########"+"Tmp_is_execute: " + tmap.is_execute);

            //AM-Orchestration ZJY
            LOG.info("Succeeded to start Container " + containerId);

            LOG.info("###########nodeid of " + containerId + " is " + container.getNodeId());
            LOG.info("###########serviceport of " + containerId.toString() + " is " + port.getServicePort());

            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(
                        containerId, container.getNodeId());
            }
            if(applicationMaster.timelineClient != null) {
                if (applicationMaster.timelineServiceV2) {
                    applicationMaster.publishContainerStartEventOnTimelineServiceV2(
                            container);
                } else {
                    applicationMaster.publishContainerStartEvent(
                            applicationMaster.timelineClient, container,
                            applicationMaster.domainId, applicationMaster.appSubmitterUgi);
                }
            }
        }

        @Override
        public void onContainerResourceIncreased(
                ContainerId containerId, Resource resource) {}

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }

        @Override
        public void onGetContainerStatusError(
                ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }

        @Override
        public void onIncreaseContainerResourceError(
                ContainerId containerId, Throwable t) {}

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
            //to avoid error
        }

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
            //to avoid error
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        private Container container;
        private String shellId;
        private int position;
        NMCallbackHandler containerListener;

        /**
         * @param lcontainer Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener, String shellId) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.shellId = shellId;
        }

        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener, String shellId,
                                        int position) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.shellId = shellId;
            this.position = position;
        }

        @Override
        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            task_container_map t_map = task_maps.get(position);
            LOG.info("### " + t_map.getCname() + " begin to execute");
            LOG.info("Setting up container launch container for containerid="
                    + container.getId() + " with shellid=" + shellId);
            while(t_map.is_pre_exec() == false){
                try {
                    Thread.sleep(1000);
                }
                catch (Exception e){
                    LOG.warn(e);
                }
            }
   
            // Set the local resources
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();


            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);

            // Set executable command
            //vargs.add(shellCommand);
            /*
            // Set shell script path
            if (!scriptPath.isEmpty()) {
                vargs.add(Shell.WINDOWS ? EXEC_BAT_SCRIPT_STRING_PATH
                        : EXEC_SHELL_STRING_PATH);
            }

            // Set args for the shell command if any
            vargs.add(shellArgs);
            */
            vargs.add(t_map.get_executor());
            LOG.info("execute command is " + t_map.get_executor());
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");


            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());
            String log_command = "";
            for (String tmp :commands){
                log_command += tmp;
            }

            // Set up ContainerLaunchContext, setting local resource, environment,
            // command and token for constructor.

            // Note for tokens: Set up tokens for the container too. Today, for normal
            // shell commands, the container in distribute-shell doesn't need any
            // tokens. We are populating them mainly for NodeManagers to be able to
            // download anyfiles in the distributed file-system. The tokens are
            // otherwise also useful in cases, for e.g., when one is running a
            // "hadoop dfs" command inside the distributed shell.
            Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
            myShellEnv.put(YARN_SHELL_ID, shellId);
            /***
             * set the containers' type for tasks, here we set docker as default
             */
            myShellEnv.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
            myShellEnv.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", t_map.getImagename());
            //thie env will be check at NM to distinguish online or offline.mix means 混合
            myShellEnv.put("YARN_CONTAINER_MIX_TYPE","online");
            //AM-Orchestration ZJY , test port mapping
            myShellEnv.put("PROTMAP", "hh");
            //AM-Orchestration ZJY , add hostname mapping
            //myShellEnv.put("ADD_HOST_NUM", "1");
            myShellEnv.put("ADD_HOST_NUM" , Integer.toString(DSConstants.HostMap.size()));
            //Added by XMQ 18.3.26 : get hostname and ip in a hashmap
            int loop_num = 0;
            for (Map.Entry<String, String> entry : DSConstants.HostMap.entrySet()){
                myShellEnv.put( "ADD_HOST_"+Integer.toString(++loop_num), entry.getKey()+ ":" +entry.getValue());
                LOG.info("##########XMQ LOG##########"
                        + " ADD_HOST_" + loop_num + ":"
                        + " Host name is: " + entry.getKey()
                        + " Host IP is: " + entry.getValue());
            }

            LOG.info("###ZJY t_map port is " + t_map.getPort());
            //AM-Orchestration ZJY, add container port requirement
            myShellEnv.put("YARN_CONTAINER_PORT", t_map.getPort());

            //Added by XMQ 2018.3.26: get environments for different service
            //added by junqing xiao 18.4.9 PORT_SYN default value is false
            switch (t_map.getImagename()){
                case "config" :
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD",DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_FALSE);
                    LOG.info("###########XMQ LOG#############Giving config environments"
                            +" CONFIG_SERVICE_PASSWORD: " + myShellEnv.get("CONFIG_SERVICE_PASSWORD"));
                    break;
                case "registry" :
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD",DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_FALSE);
                    LOG.info("###########XMQ LOG#############Giving registry environments"
                            + " CONFIG_HOST: " + myShellEnv.get("CONFIG_HOST")
                            + " CONFIG_PORT: " + myShellEnv.get("CONFIG_PORT")
                            + " CONFIG_SERVICE_PASSWORD: " + myShellEnv.get("CONFIG_SERVICE_PASSWORD"));
                    break;
                case "auth-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("NOTIFICATION_SERVICE_PASSWORD" , DSConstants.NOTIFICATION_SERVICE_PASSWORD);
                    myShellEnv.put("STATISTICS_SERVICE_PASSWORD" , DSConstants.STATISTICS_SERVICE_PASSWORD);
                    myShellEnv.put("ACCOUNT_SERVICE_PASSWORD" , DSConstants.ACCOUNT_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST)); // need to be given
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving auth-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT);
                    break;
                case "account-mongo-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD",DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("INIT_DUMP" , "/account-service-dump.js");
                    myShellEnv.put("NOTIFICATION_SERVICE_PORT" , DSConstants.NOTIFICATION_SERVICE_PASSWORD);
                    myShellEnv.put("STATISTICS_SERVICE_PASSWORD" , DSConstants.STATISTICS_SERVICE_PASSWORD);
                    myShellEnv.put("ACCOUNT_SERVICE_PASSWORD" , DSConstants.ACCOUNT_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving auth-mongo-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "account-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD",DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("ACCOUNT_SERVICE_PASSWORD" , DSConstants.ACCOUNT_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving account-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "gateway" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD",DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving gateway environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "statistics-mongo-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("NOTIFICATION_SERVICE_PASSWORD" , DSConstants.NOTIFICATION_SERVICE_PASSWORD);
                    myShellEnv.put("STATISTICS_SERVICE_PASSWORD" , DSConstants.STATISTICS_SERVICE_PASSWORD);
                    myShellEnv.put("ACCOUNT_SERVICE_PASSWORD" , DSConstants.ACCOUNT_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving statistics-mongo-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "statistics-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("STATISTICS_SERVICE_PASSWORD" , DSConstants.STATISTICS_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving statistics-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "notification-mongo-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("NOTIFICATION_SERVICE_PASSWORD" , DSConstants.NOTIFICATION_SERVICE_PASSWORD);
                    myShellEnv.put("STATISTICS_SERVICE_PASSWORD" , DSConstants.STATISTICS_SERVICE_PASSWORD);
                    myShellEnv.put("ACCOUNT_SERVICE_PASSWORD" , DSConstants.ACCOUNT_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving notification-mongo-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "notification-service" :
                    myShellEnv.put("REDIS_IP" , DSConstants.REDIS_IP);
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("NOTIFICATION_SERVICE_PASSWORD" , DSConstants.NOTIFICATION_SERVICE_PASSWORD);
                    myShellEnv.put("MONGODB_PASSWORD" , DSConstants.MONGODB_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("AUTH_SERVICE_HOST" , "auth-service:"+DSConstants.HostMap.get(AUTH_SERVICE_HOST));
                    myShellEnv.put("AUTH_SERVICE_PORT" , AUTH_SERVICE_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_TRUE);
                    LOG.info("###########XMQ LOG##########Giving notification-service environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT
                            + " AUTH_SERVICE_HOST: " + "auth-service:" + DSConstants.HostMap.get(AUTH_SERVICE_HOST)
                            + " AUTH_SERVICE_PORT: " + AUTH_SERVICE_PORT);
                    break;
                case "monitoring" :
                    myShellEnv.put("CONFIG_SERVICE_PASSWORD" , DSConstants.CONFIG_SERVICE_PASSWORD);
                    myShellEnv.put("CONFIG_HOST","config:"+DSConstants.HostMap.get(CONFIG_HOST));
                    myShellEnv.put("CONFIG_PORT",CONFIG_PORT);
                    myShellEnv.put("REGISTRY_HOST" , "registry:"+DSConstants.HostMap.get(REGISTRY_HOST));
                    myShellEnv.put("REGISTRY_PORT" , REGISTRY_PORT);
                    myShellEnv.put("RABBITMQ_HOST" , "rabbitmq:"+DSConstants.HostMap.get(RABBITMQ_HOST));
                    myShellEnv.put("RABBITMQ_PORT" , RABBITMQ_PORT);
                    myShellEnv.put("PORT_SYN",DSConstants.PORT_SYN_FALSE);
                    LOG.info("###########XMQ LOG##########Giving monitoring environments"
                            + " CONFIG_HOST: " + "config:" + DSConstants.HostMap.get(CONFIG_HOST)
                            + " CONFIG_PORT: " + CONFIG_PORT
                            + " REGISTRY_HOST: " + "registry:" + DSConstants.HostMap.get(REGISTRY_HOST)
                            + " REGISTRY_PORT: " + REGISTRY_PORT
                            + " RABBITMQ_HOST: " + "rabbitmq:" + DSConstants.HostMap.get(RABBITMQ_HOST)
                            + " RABBITMQ_PORT: " + RABBITMQ_PORT);
                    break;
            }

            ContainerRetryContext containerRetryContext =
                    ContainerRetryContext.newInstance(
                            containerRetryPolicy, containerRetryErrorCodes,
                            containerMaxRetries, containrRetryInterval);
//            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
//                    localResources, myShellEnv, commands, null, allTokens.duplicate(),
//                    null, containerRetryContext);
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                    localResources, myShellEnv, commands, null, allTokens.duplicate(),
                    null, containerRetryContext);
            containerListener.addContainer(container.getId(), container);

            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    private void renameScriptFile(final Path renamedScriptPath)
            throws IOException, InterruptedException {
        appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws IOException {
                FileSystem fs = renamedScriptPath.getFileSystem(conf);
                fs.rename(new Path(scriptPath), renamedScriptPath);
                return null;
            }
        });
        LOG.info("User " + appSubmitterUgi.getUserName()
                + " added suffix(.sh/.bat) to script file as " + renamedScriptPath);
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    // AM-Orchestration ZJY
    private ContainerRequest setupContainerAskForRM(int cpu ,long mem,MixType  mixType, ServicePort Port) {
        LOG.info("ready to require cpu and mem is " + cpu + " " + mem);
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        // TODO - what is the range for priority? how to decide?
        Priority pri = Priority.newInstance(requestPriority);

        // Set up resource type requirements
        // For now, memory and CPU are supported so we set memory and cpu requirements

        Resource capability = Resource.newInstance(mem,
                cpu);

        ContainerRequest request = new ContainerRequest(capability, null, null, pri,mixType, Port);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }

    private boolean fileExist(String filePath) {
        return new File(filePath).exists();
    }

    private String readContent(String filePath) throws IOException {
        DataInputStream ds = null;
        try {
            ds = new DataInputStream(new FileInputStream(filePath));
            return ds.readUTF();
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(ds);
        }
    }

    private void publishContainerStartEvent(
            final TimelineClient timelineClient, final Container container,
            String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
        entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME, container.getId()
                .getApplicationAttemptId().getApplicationId().toString());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_START.toString());
        event.addEventInfo("Node", container.getNodeId().toString());
        event.addEventInfo("Resources", container.getResource().toString());
        entity.addEvent(event);

        try {
            processTimelineResponseErrors(
                    putContainerEntity(timelineClient,
                            container.getId().getApplicationAttemptId(),
                            entity));
        } catch (YarnException | IOException | ClientHandlerException e) {
            LOG.error("Container start event could not be published for "
                    + container.getId().toString(), e);
        }
    }


    void publishContainerEndEvent(
            final TimelineClient timelineClient, ContainerStatus container,
            String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(container.getContainerId().toString());
        entity.setEntityType(DSEntity.DS_CONTAINER.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
        entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
                container.getContainerId().getApplicationAttemptId()
                        .getApplicationId().toString());
        TimelineEvent event = new TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setEventType(DSEvent.DS_CONTAINER_END.toString());
        event.addEventInfo("State", container.getState().name());
        event.addEventInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);
        try {
            processTimelineResponseErrors(
                    putContainerEntity(timelineClient,
                            container.getContainerId().getApplicationAttemptId(),
                            entity));
        } catch (YarnException | IOException | ClientHandlerException e) {
            LOG.error("Container end event could not be published for "
                    + container.getContainerId().toString(), e);
        }
    }

    private TimelinePutResponse putContainerEntity(
            TimelineClient timelineClient, ApplicationAttemptId currAttemptId,
            TimelineEntity entity)
            throws YarnException, IOException {
        if (TimelineUtils.timelineServiceV1_5Enabled(conf)) {
            TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(
                    currAttemptId.getApplicationId(),
                    CONTAINER_ENTITY_GROUP_ID);
            return timelineClient.putEntities(currAttemptId, groupId, entity);
        } else {
            return timelineClient.putEntities(entity);
        }
    }

    private void publishApplicationAttemptEvent(
            final TimelineClient timelineClient, String appAttemptId,
            DSEvent appEvent, String domainId, UserGroupInformation ugi) {
        final TimelineEntity entity = new TimelineEntity();
        entity.setEntityId(appAttemptId);
        entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
        entity.setDomainId(domainId);
        entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
        TimelineEvent event = new TimelineEvent();
        event.setEventType(appEvent.toString());
        event.setTimestamp(System.currentTimeMillis());
        entity.addEvent(event);
        try {
            TimelinePutResponse response = timelineClient.putEntities(entity);
            processTimelineResponseErrors(response);
        } catch (YarnException | IOException | ClientHandlerException e) {
            LOG.error("App Attempt "
                    + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
                    + " event could not be published for "
                    + appAttemptID, e);
        }
    }

    private TimelinePutResponse processTimelineResponseErrors(
            TimelinePutResponse response) {
        List<TimelinePutResponse.TimelinePutError> errors = response.getErrors();
        if (errors.size() == 0) {
            LOG.debug("Timeline entities are successfully put");
        } else {
            for (TimelinePutResponse.TimelinePutError error : errors) {
                LOG.error(
                        "Error when publishing entity [" + error.getEntityType() + ","
                                + error.getEntityId() + "], server side error code: "
                                + error.getErrorCode());
            }
        }
        return response;
    }

    RMCallbackHandler getRMCallbackHandler() {
        return new RMCallbackHandler();
    }

    void setAmRMClient(AMRMClientAsync client) {
        this.amRMClient = client;
    }

    int getNumCompletedContainers() {
        return numCompletedContainers.get();
    }

    boolean getDone() {
        return done;
    }

    Thread createLaunchContainerThread(Container allocatedContainer,
                                       String shellId) {
        LaunchContainerRunnable runnableLaunchContainer =
                new LaunchContainerRunnable(allocatedContainer, containerListener,
                        shellId);
        return new Thread(runnableLaunchContainer);
    }

    Thread createLaunchContainerThread(Container allocatedContainer,
                                       String shellId,int index) {
        LaunchContainerRunnable runnableLaunchContainer =
                new LaunchContainerRunnable(allocatedContainer, containerListener,
                        shellId,index);
        return new Thread(runnableLaunchContainer);
    }

    private void publishContainerStartEventOnTimelineServiceV2(
            Container container) {
        final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
                entity =
                new org.apache.hadoop.yarn.api.records.timelineservice.
                        TimelineEntity();
        entity.setId(container.getId().toString());
        entity.setType(DSEntity.DS_CONTAINER.toString());
        long ts = System.currentTimeMillis();
        entity.setCreatedTime(ts);
        entity.addInfo("user", appSubmitterUgi.getShortUserName());

        org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
                new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
        event.setTimestamp(ts);
        event.setId(DSEvent.DS_CONTAINER_START.toString());
        event.addInfo("Node", container.getNodeId().toString());
        event.addInfo("Resources", container.getResource().toString());
        entity.addEvent(event);

        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    timelineV2Client.putEntities(entity);
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("Container start event could not be published for "
                            + container.getId().toString(),
                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
        }
    }

    private void publishContainerEndEventOnTimelineServiceV2(
            final ContainerStatus container) {
        final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
                entity =
                new org.apache.hadoop.yarn.api.records.timelineservice.
                        TimelineEntity();
        entity.setId(container.getContainerId().toString());
        entity.setType(DSEntity.DS_CONTAINER.toString());
        //entity.setDomainId(domainId);
        entity.addInfo("user", appSubmitterUgi.getShortUserName());
        org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
                new  org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
        event.setTimestamp(System.currentTimeMillis());
        event.setId(DSEvent.DS_CONTAINER_END.toString());
        event.addInfo("State", container.getState().name());
        event.addInfo("Exit Status", container.getExitStatus());
        entity.addEvent(event);

        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    timelineV2Client.putEntities(entity);
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("Container end event could not be published for "
                            + container.getContainerId().toString(),
                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
        }
    }

    private void publishApplicationAttemptEventOnTimelineServiceV2(
            DSEvent appEvent) {
        final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
                entity =
                new org.apache.hadoop.yarn.api.records.timelineservice.
                        TimelineEntity();
        entity.setId(appAttemptID.toString());
        entity.setType(DSEntity.DS_APP_ATTEMPT.toString());
        long ts = System.currentTimeMillis();
        if (appEvent == DSEvent.DS_APP_ATTEMPT_START) {
            entity.setCreatedTime(ts);
        }
        entity.addInfo("user", appSubmitterUgi.getShortUserName());
        org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
                new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
        event.setId(appEvent.toString());
        event.setTimestamp(ts);
        entity.addEvent(event);

        try {
            appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public TimelinePutResponse run() throws Exception {
                    timelineV2Client.putEntitiesAsync(entity);
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("App Attempt "
                            + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
                            + " event could not be published for "
                            + appAttemptID,
                    e instanceof UndeclaredThrowableException ? e.getCause() : e);
        }
    }

}
