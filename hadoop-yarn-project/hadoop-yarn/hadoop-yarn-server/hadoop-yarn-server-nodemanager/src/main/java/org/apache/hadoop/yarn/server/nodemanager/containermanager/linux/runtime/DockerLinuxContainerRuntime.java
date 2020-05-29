/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ServicePort;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerInspectCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerStopCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

/**
 * <p>This class is a {@link ContainerRuntime} implementation that uses the
 * native {@code container-executor} binary via a
 * {@link PrivilegedOperationExecutor} instance to launch processes inside
 * Docker containers.</p>
 *
 * <p>The following environment variables are used to configure the Docker
 * engine:</p>
 *
 * <ul>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_TYPE} ultimately determines whether a
 *     Docker container will be used. If the value is {@code docker}, a Docker
 *     container will be used. Otherwise a regular process tree container will
 *     be used. This environment variable is checked by the
 *     {@link #isDockerContainerRequested} method, which is called by the
 *     {@link DelegatingLinuxContainerRuntime}.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_IMAGE} names which image
 *     will be used to launch the Docker container.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_FILE} is currently ignored.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE} controls
 *     whether the Docker container's default command is overridden.  When set
 *     to {@code true}, the Docker container's command will be
 *     {@code bash <path_to_launch_script>}. When unset or set to {@code false}
 *     the Docker container's default command is used.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK} sets the
 *     network type to be used by the Docker container. It must be a valid
 *     value as determined by the
 *     {@code yarn.nodemanager.runtime.linux.docker.allowed-container-networks}
 *     property.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME} sets the
 *     hostname to be used by the Docker container. If not specified, a
 *     hostname will be derived from the container ID.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER}
 *     controls whether the Docker container is a privileged container. In order
 *     to use privileged containers, the
 *     {@code yarn.nodemanager.runtime.linux.docker.privileged-containers.allowed}
 *     property must be set to {@code true}, and the application owner must
 *     appear in the value of the
 *     {@code yarn.nodemanager.runtime.linux.docker.privileged-containers.acl}
 *     property. If this environment variable is set to {@code true}, a
 *     privileged Docker container will be used if allowed. No other value is
 *     allowed, so the environment variable should be left unset rather than
 *     setting it to false.
 *   </li>
 *   <li>
 *     {@code YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS} adds
 *     additional volume mounts to the Docker container. The value of the
 *     environment variable should be a comma-separated list of mounts.
 *     All such mounts must be given as {@code source:dest}, where the
 *     source is an absolute path that is not a symlink and that points to a
 *     localized resource.
 *   </li>
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Logger LOG =
          LoggerFactory.getLogger(DockerLinuxContainerRuntime.class);

  // This validates that the image is a proper docker image
  public static final String DOCKER_IMAGE_PATTERN =
          "^(([a-zA-Z0-9.-]+)(:\\d+)?/)?([a-z0-9_./-]+)(:[\\w.-]+)?$";
  private static final Pattern dockerImagePattern =
          Pattern.compile(DOCKER_IMAGE_PATTERN);
  public static final String HOSTNAME_PATTERN =
          "^[a-zA-Z0-9][a-zA-Z0-9_.-]+$";
  private static final Pattern hostnamePattern = Pattern.compile(
          HOSTNAME_PATTERN);

  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE =
          "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_IMAGE_FILE =
          "YARN_CONTAINER_RUNTIME_DOCKER_IMAGE_FILE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE =
          "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_NETWORK =
          "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_HOSTNAME =
          "YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER =
          "YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_RUN_ENABLE_USER_REMAPPING =
          "YARN_CONTAINER_RUNTIME_DOCKER_RUN_ENABLE_USER_REMAPPING";
  @InterfaceAudience.Private
  public static final String ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS =
          "YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS";

  private Configuration conf;
  private DockerClient dockerClient;
  private PrivilegedOperationExecutor privilegedOperationExecutor;
  private Set<String> allowedNetworks = new HashSet<>();
  private String defaultNetwork;
  private String cgroupsRootDirectory;
  private CGroupsHandler cGroupsHandler;
  private AccessControlList privilegedContainersAcl;
  private boolean enableUserReMapping;
  private int userRemappingUidThreshold;
  private int userRemappingGidThreshold;

  /**
   * Return whether the given environment variables indicate that the operation
   * is requesting a Docker container.  If the environment contains a key
   * called {@code YARN_CONTAINER_RUNTIME_TYPE} whose value is {@code docker},
   * this method will return true.  Otherwise it will return false.
   *
   * @param env the environment variable settings for the operation
   * @return whether a Docker container is requested
   */
  public static boolean isDockerContainerRequested(
          Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);

    return type != null && type.equals("docker");
  }

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   */
  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
                                             privilegedOperationExecutor) {
    this(privilegedOperationExecutor,
            ResourceHandlerModule.getCGroupsHandler());
  }

  /**
   * Create an instance using the given {@link PrivilegedOperationExecutor}
   * instance for performing operations and the given {@link CGroupsHandler}
   * instance. This constructor is intended for use in testing.
   *
   * @param privilegedOperationExecutor the {@link PrivilegedOperationExecutor}
   * instance
   * @param cGroupsHandler the {@link CGroupsHandler} instance
   */
  @VisibleForTesting
  public DockerLinuxContainerRuntime(PrivilegedOperationExecutor
                                             privilegedOperationExecutor, CGroupsHandler cGroupsHandler) {
    this.privilegedOperationExecutor = privilegedOperationExecutor;

    if (cGroupsHandler == null) {
      LOG.info("cGroupsHandler is null - cgroups not in use.");
    } else {
      this.cGroupsHandler = cGroupsHandler;
      this.cgroupsRootDirectory = cGroupsHandler.getCGroupMountPath();
    }
  }

  @Override
  public void initialize(Configuration conf)
          throws ContainerExecutionException {
    this.conf = conf;
    dockerClient = new DockerClient(conf);
    allowedNetworks.clear();
    allowedNetworks.addAll(Arrays.asList(
            conf.getTrimmedStrings(
                    YarnConfiguration.NM_DOCKER_ALLOWED_CONTAINER_NETWORKS,
                    YarnConfiguration.DEFAULT_NM_DOCKER_ALLOWED_CONTAINER_NETWORKS)));
    defaultNetwork = conf.getTrimmed(
            YarnConfiguration.NM_DOCKER_DEFAULT_CONTAINER_NETWORK,
            YarnConfiguration.DEFAULT_NM_DOCKER_DEFAULT_CONTAINER_NETWORK);

    if(!allowedNetworks.contains(defaultNetwork)) {
      String message = "Default network: " + defaultNetwork
              + " is not in the set of allowed networks: " + allowedNetworks;

      if (LOG.isWarnEnabled()) {
        LOG.warn(message + ". Please check "
                + "configuration");
      }

      throw new ContainerExecutionException(message);
    }

    privilegedContainersAcl = new AccessControlList(conf.getTrimmed(
            YarnConfiguration.NM_DOCKER_PRIVILEGED_CONTAINERS_ACL,
            YarnConfiguration.DEFAULT_NM_DOCKER_PRIVILEGED_CONTAINERS_ACL));

    enableUserReMapping = conf.getBoolean(
            YarnConfiguration.NM_DOCKER_ENABLE_USER_REMAPPING,
            YarnConfiguration.DEFAULT_NM_DOCKER_ENABLE_USER_REMAPPING);

    userRemappingUidThreshold = conf.getInt(
            YarnConfiguration.NM_DOCKER_USER_REMAPPING_UID_THRESHOLD,
            YarnConfiguration.DEFAULT_NM_DOCKER_USER_REMAPPING_UID_THRESHOLD);

    userRemappingGidThreshold = conf.getInt(
            YarnConfiguration.NM_DOCKER_USER_REMAPPING_GID_THRESHOLD,
            YarnConfiguration.DEFAULT_NM_DOCKER_USER_REMAPPING_GID_THRESHOLD);
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
          throws ContainerExecutionException {
  }

  private void validateContainerNetworkType(String network)
          throws ContainerExecutionException {
    if (allowedNetworks.contains(network)) {
      return;
    }

    String msg = "Disallowed network:  '" + network
            + "' specified. Allowed networks: are " + allowedNetworks
            .toString();
    throw new ContainerExecutionException(msg);
  }

  public static void validateHostname(String hostname) throws
          ContainerExecutionException {
    if (hostname != null && !hostname.isEmpty()) {
      if (!hostnamePattern.matcher(hostname).matches()) {
        throw new ContainerExecutionException("Hostname '" + hostname
                + "' doesn't match docker hostname pattern");
      }
    }
  }

  /** Set a DNS friendly hostname. */
  private void setHostname(DockerRunCommand runCommand, String
          containerIdStr, String name)
          throws ContainerExecutionException {
    if (name == null || name.isEmpty()) {
      name = RegistryPathUtils.encodeYarnID(containerIdStr);
      validateHostname(name);
    }

    LOG.info("setting hostname in container to: " + name);
    runCommand.setHostname(name);
  }

  /**
   * If CGROUPS in enabled and not set to none, then set the CGROUP parent for
   * the command instance.
   *
   * @param resourcesOptions the resource options to check for "cgroups=none"
   * @param containerIdStr the container ID
   * @param runCommand the command to set with the CGROUP parent
   */
  @VisibleForTesting
  protected void addCGroupParentIfRequired(String resourcesOptions,
                                           String containerIdStr, DockerRunCommand runCommand) {
    if (cGroupsHandler == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cGroupsHandler is null. cgroups are not in use. nothing to"
                + " do.");
      }
      return;
    }

    if (resourcesOptions.equals(PrivilegedOperation.CGROUP_ARG_PREFIX
            + PrivilegedOperation.CGROUP_ARG_NO_TASKS)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("no resource restrictions specified. not using docker's "
                + "cgroup options");
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("using docker's cgroups options");
      }

      String cGroupPath = "/"
              + cGroupsHandler.getRelativePathForCGroup(containerIdStr);

      if (LOG.isDebugEnabled()) {
        LOG.debug("using cgroup parent: " + cGroupPath);
      }

      runCommand.setCGroupParent(cGroupPath);
    }
  }

  /**
   * Return whether the YARN container is allowed to run in a privileged
   * Docker container. For a privileged container to be allowed all of the
   * following three conditions must be satisfied:
   *
   * <ol>
   *   <li>Submitting user must request for a privileged container</li>
   *   <li>Privileged containers must be enabled on the cluster</li>
   *   <li>Submitting user must be white-listed to run a privileged
   *   container</li>
   * </ol>
   *
   * @param container the target YARN container
   * @return whether privileged container execution is allowed
   * @throws ContainerExecutionException if privileged container execution
   * is requested but is not allowed
   */
  private boolean allowPrivilegedContainerExecution(Container container)
          throws ContainerExecutionException {
    Map<String, String> environment = container.getLaunchContext()
            .getEnvironment();
    String runPrivilegedContainerEnvVar = environment
            .get(ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER);

    if (runPrivilegedContainerEnvVar == null) {
      return false;
    }

    if (!runPrivilegedContainerEnvVar.equalsIgnoreCase("true")) {
      LOG.warn("NOT running a privileged container. Value of " +
              ENV_DOCKER_CONTAINER_RUN_PRIVILEGED_CONTAINER
              + "is invalid: " + runPrivilegedContainerEnvVar);
      return false;
    }

    LOG.info("Privileged container requested for : " + container
            .getContainerId().toString());

    //Ok, so we have been asked to run a privileged container. Security
    // checks need to be run. Each violation is an error.

    //check if privileged containers are enabled.
    boolean privilegedContainersEnabledOnCluster = conf.getBoolean(
            YarnConfiguration.NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS,
            YarnConfiguration.DEFAULT_NM_DOCKER_ALLOW_PRIVILEGED_CONTAINERS);

    if (!privilegedContainersEnabledOnCluster) {
      String message = "Privileged container being requested but privileged "
              + "containers are not enabled on this cluster";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    //check if submitting user is in the whitelist.
    String submittingUser = container.getUser();
    UserGroupInformation submitterUgi = UserGroupInformation
            .createRemoteUser(submittingUser);

    if (!privilegedContainersAcl.isUserAllowed(submitterUgi)) {
      String message = "Cannot launch privileged container. Submitting user ("
              + submittingUser + ") fails ACL check.";
      LOG.warn(message);
      throw new ContainerExecutionException(message);
    }

    LOG.info("All checks pass. Launching privileged container for : "
            + container.getContainerId().toString());

    return true;
  }

  @VisibleForTesting
  protected String validateMount(String mount,
                                 Map<Path, List<String>> localizedResources)
          throws ContainerExecutionException {
    for (Entry<Path, List<String>> resource : localizedResources.entrySet()) {
      if (resource.getValue().contains(mount)) {
        java.nio.file.Path path = Paths.get(resource.getKey().toString());
        if (!path.isAbsolute()) {
          throw new ContainerExecutionException("Mount must be absolute: " +
                  mount);
        }
        if (Files.isSymbolicLink(path)) {
          throw new ContainerExecutionException("Mount cannot be a symlink: " +
                  mount);
        }
        return path.toString();
      }
    }
    throw new ContainerExecutionException("Mount must be a localized " +
            "resource: " + mount);
  }

  private String getUserIdInfo(String userName)
          throws ContainerExecutionException {
    String id = "";
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
            new String[]{"id", "-u", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replaceAll("[^0-9]", "");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
  }

  private String[] getGroupIdInfo(String userName)
          throws ContainerExecutionException {
    String[] id = null;
    Shell.ShellCommandExecutor shexec = new Shell.ShellCommandExecutor(
            new String[]{"id", "-G", userName});
    try {
      shexec.execute();
      id = shexec.getOutput().replace("\n", "").split(" ");
    } catch (Exception e) {
      throw new ContainerExecutionException(e);
    }
    return id;
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
          throws ContainerExecutionException {
    Container container = ctx.getContainer();
    Map<String, String> environment = container.getLaunchContext()
            .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);
    String network = environment.get(ENV_DOCKER_CONTAINER_NETWORK);
    String hostname = environment.get(ENV_DOCKER_CONTAINER_HOSTNAME);

    if(network == null || network.isEmpty()) {
      //AM-Orchestration ZJY
      if (environment.get("YARN_CONTAINER_PORT") != null){
        network = "bridge";
      }else {
        network = defaultNetwork;
      }
    }

    validateContainerNetworkType(network);

    validateHostname(hostname);

    validateImageName(imageName);

    String containerIdStr = container.getContainerId().toString();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String dockerRunAsUser = runAsUser;
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    String[] groups = null;

    if (enableUserReMapping) {
      String uid = getUserIdInfo(runAsUser);
      groups = getGroupIdInfo(runAsUser);
      String gid = groups[0];
      if(Integer.parseInt(uid) < userRemappingUidThreshold) {
        String message = "uid: " + uid + " below threshold: "
                + userRemappingUidThreshold;
        throw new ContainerExecutionException(message);
      }
      for(int i = 0; i < groups.length; i++) {
        String group = groups[i];
        if (Integer.parseInt(group) < userRemappingGidThreshold) {
          String message = "gid: " + group
                  + " below threshold: " + userRemappingGidThreshold;
          throw new ContainerExecutionException(message);
        }
      }
      dockerRunAsUser = uid + ":" + gid;
    }

    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> filecacheDirs = ctx.getExecutionAttribute(FILECACHE_DIRS);
    @SuppressWarnings("unchecked")
    List<String> containerLocalDirs = ctx.getExecutionAttribute(
            CONTAINER_LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> containerLogDirs = ctx.getExecutionAttribute(
            CONTAINER_LOG_DIRS);
    @SuppressWarnings("unchecked")
    Map<Path, List<String>> localizedResources = ctx.getExecutionAttribute(
            LOCALIZED_RESOURCES);
    @SuppressWarnings("unchecked")
    List<String> userLocalDirs = ctx.getExecutionAttribute(USER_LOCAL_DIRS);
    Set<String> capabilities = new HashSet<>(Arrays.asList(
            conf.getTrimmedStrings(
                    YarnConfiguration.NM_DOCKER_CONTAINER_CAPABILITIES,
                    YarnConfiguration.DEFAULT_NM_DOCKER_CONTAINER_CAPABILITIES)));

    @SuppressWarnings("unchecked")
    DockerRunCommand runCommand = new DockerRunCommand(containerIdStr,
            dockerRunAsUser, imageName)
            .detachOnRun()
            .setContainerWorkDir(containerWorkDir.toString())
            .setNetworkType(network);
    setHostname(runCommand, containerIdStr, hostname);
    LOG.info("AM-Orchestration ZJY , network type is " + network);
    runCommand.setCapabilities(capabilities);


    /**
     * AM-Orchestration ZJY
     * get nmcontex containeridmap and implement virtual port
     */
    //Iterator it = (Iterator) ((NodeManager.NMContext)ctx.getContext()).getContainerId_port().entrySet();
    if (environment.get("YARN_CONTAINER_PORT") != null){

      String curr_port = environment.get("YARN_CONTAINER_PORT");
      LOG.info("AM-Orchestartion ZJY : port " + curr_port + " may need to be virtualized");
      int max_port = 0; boolean occupy = false;
      for(Map.Entry<String, Integer> entry : ((NodeManager.NMContext)ctx.getContext()).getContainerId_port().entrySet())
      {
        LOG.info("< " + entry.getKey() + "," + entry.getValue() + "> exists already");
        int existport = entry.getValue();
        int tmp_port = Integer.valueOf(curr_port.trim());
        max_port = entry.getValue() > max_port ? entry.getValue():max_port;
        if (existport == tmp_port){
          LOG.info("AM-Orchestration ZJY : port " + curr_port + " is occupied");
          occupy = true;
        }
      }
      if (occupy){
        int real_port = max_port + 1;
        curr_port = String.valueOf(real_port);
      }else{
        LOG.info("AM-Orchestration ZJY : " + curr_port + " is not occupied");
      }
      Map<String, Integer> t_map = ((NodeManager.NMContext)ctx.getContext()).getContainerId_port();
      t_map.put(container.getContainerId().toString(), Integer.parseInt(curr_port));
      ((NodeManager.NMContext)ctx.getContext()).setContainerId_port(t_map);

      //Added by junqing xiao 18.4.9  only one port now
      //use env PORT_SYN to judge change expose port or not
      if ( environment.get("PORT_SYN") != null ){
        if ( "true".equals(environment.get("PORT_SYN")) ){
          Set<String> ports = new HashSet<>();
          ports.add(curr_port);
          runCommand.addExpose(ports);
          runCommand.addPortMap(curr_port, curr_port);
        }else if ( "false".equals(environment.get("PORT_SYN")) ){
          runCommand.addPortMap(curr_port, environment.get("YARN_CONTAINER_PORT"));
        }else {
          LOG.warn("**junqing xiao**illegal argument with PORT_SYN");
          throw new IllegalArgumentException("**junqing xiao**illegal argument with PORT_SYN");
        }
      }else{
        runCommand.addPortMap(curr_port, environment.get("YARN_CONTAINER_PORT"));
      }
      //Added by XMQ 2018.3.28
      runCommand.addEnvKV("SERVICE_PORT", curr_port);
      if (environment.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE").equals("auth-service")){
        runCommand.addEnvKV("AUTH_SERVICE_PORT" , curr_port);
        LOG.info("########XMQ LOG######### " + environment.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE")
                + "'s environment AUTH_SERVICE_PORT = " + curr_port);}
      //Added by XMQ 2018.4.2: add registry port
      if (environment.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE").equals("registry")){
        runCommand.addEnvKV("REGISTRY_PORT" , curr_port);
        LOG.info("########XMQ LOG#########" + environment.get("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE")
                + "'s environment REGISTRY_PORT = " + curr_port);}

      for(Map.Entry<String, Integer> entry : ((NodeManager.NMContext)ctx.getContext()).getContainerId_port().entrySet()) {
        LOG.info("< " + entry.getKey() + "," + entry.getValue() + "> exists now");
      }
    }
    //added by junqing xiao 18.4.10
    if (environment.get("REDIS_IP") != null){
      runCommand.addEnvKV("REDIS_IP" , environment.get("REDIS_IP"));
    }

    //Added by XMQ 18.3.25
    if (environment.get("RABBITMQ_PORT") != null){
      runCommand.addEnvKV("RABBITMQ_PORT" , environment.get("RABBITMQ_PORT"));
    }

    if (environment.get("RABBITMQ_HOST") != null){
      runCommand.addHost(environment.get("RABBITMQ_HOST"));
    }

    if (environment.get("CONFIG_PORT") != null){
      runCommand.addEnvKV("CONFIG_PORT" , environment.get("CONFIG_PORT"));
    }

    if(environment.get("CONFIG_HOST") != null){
      runCommand.addHost(environment.get("CONFIG_HOST"));
    }

    if (environment.get("REGISTRY_HOST") != null){
      runCommand.addHost(environment.get("REGISTRY_HOST"));
    }

    if (environment.get("REGISTRY_PORT") != null){
      runCommand.addEnvKV("REGISTRY_PORT" , environment.get("REGISTRY_PORT"));
    }

    if (environment.get("AUTH_SERVICE_HOST") != null){
      runCommand.addHost(environment.get("AUTH_SERVICE_HOST"));
    }

    if (environment.get("AUTH_SERVICE_PORT") != null){
      runCommand.addEnvKV("AUTH_SERVICE_PORT" , environment.get("AUTH_SERVICE_PORT"));
    }

    //added by junqing xiao 18.1.16
    if (environment.get("CONFIG_SERVICE_PASSWORD") != null){
      runCommand.addEnvKV("CONFIG_SERVICE_PASSWORD",environment.get("CONFIG_SERVICE_PASSWORD"));
    }

    if (environment.get("MONGODB_PASSWORD") != null){
      runCommand.addEnvKV("MONGODB_PASSWORD",environment.get("MONGODB_PASSWORD"));
    }

    if (environment.get("NOTIFICATION_SERVICE_PASSWORD") != null){
      runCommand.addEnvKV("NOTIFICATION_SERVICE_PASSWORD",environment.get("NOTIFICATION_SERVICE_PASSWORD"));
    }
    if (environment.get("STATISTICS_SERVICE_PASSWORD") != null){
      runCommand.addEnvKV("STATISTICS_SERVICE_PASSWORD",environment.get("STATISTICS_SERVICE_PASSWORD"));
    }
    if (environment.get("ACCOUNT_SERVICE_PASSWORD") != null){
      runCommand.addEnvKV("ACCOUNT_SERVICE_PASSWORD",environment.get("ACCOUNT_SERVICE_PASSWORD"));
    }
    if (environment.get("INIT_DUMP") != null){
      runCommand.addEnvKV("INIT_DUMP",environment.get("INIT_DUMP"));
    }

    //added by junqing xiao 18.1.17
    //revised by XMQ 2018.3.28
    String Host_ip = ((NodeManager.NMContext)ctx.getContext()).getNodeId().getHost();
    String addHostNumString =  environment.get("ADD_HOST_NUM");
    if (addHostNumString != null){
      int addHostNum = Integer.valueOf(addHostNumString);
      for (int i = 1; i <= addHostNum ; i++){
        String value = environment.get("ADD_HOST_" + i);
        if (value != null){
          runCommand.addHost(value);
          String a[] = value.split(":");
          if (a[0].equals(Host_ip)){
            runCommand.addEnvKV("HOST_IP",a[1]);
          }
        }else {
          LOG.warn("junqing xiao : add host num is not right!!!");
        }
      }
    }

    if(cgroupsRootDirectory != null) {
      runCommand.addMountLocation(cgroupsRootDirectory,
              cgroupsRootDirectory + ":ro", false);
    }

    List<String> allDirs = new ArrayList<>(containerLocalDirs);
    allDirs.addAll(filecacheDirs);
    allDirs.add(containerWorkDir.toString());
    allDirs.addAll(containerLogDirs);
    allDirs.addAll(userLocalDirs);
    for (String dir: allDirs) {
      runCommand.addMountLocation(dir, dir, true);
    }

    //add by quxm 2017.10.23 "mount JAVA_HOME and HADOOP_HOME"
    runCommand.addMountLocation(conf.getTrimmed(YarnConfiguration.NM_DOCKER_CONTAINER_JAVA_HOME_MOUNT),conf.getTrimmed(YarnConfiguration.NM_DOCKER_CONTAINER_JAVA_HOME_MOUNT),true);
    runCommand.addMountLocation(conf.getTrimmed(YarnConfiguration.NM_DOCKER_CONTAINER_HADOOP_HOME_MOUNT),conf.getTrimmed(YarnConfiguration.NM_DOCKER_CONTAINER_HADOOP_HOME_MOUNT),true);
    //LOG.info("### docker java home = " + conf.getTrimmed(YarnConfiguration.NM_DOCKER_CONTAINER_JAVA_HOME_MOUNT));

    if (environment.containsKey(ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS)) {
      String mounts = environment.get(
              ENV_DOCKER_CONTAINER_LOCAL_RESOURCE_MOUNTS);
      if (!mounts.isEmpty()) {
        for (String mount : StringUtils.split(mounts)) {
          String[] dir = StringUtils.split(mount, ':');
          if (dir.length != 2) {
            throw new ContainerExecutionException("Invalid mount : " +
                    mount);
          }
          String src = validateMount(dir[0], localizedResources);
          String dst = dir[1];
          runCommand.addMountLocation(src, dst + ":ro", true);
        }
      }
    }

    if (allowPrivilegedContainerExecution(container)) {
      runCommand.setPrivileged();
    }

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    addCGroupParentIfRequired(resourcesOpts, containerIdStr, runCommand);

    String disableOverride = environment.get(
            ENV_DOCKER_CONTAINER_RUN_OVERRIDE_DISABLE);

    if (disableOverride != null && disableOverride.equals("true")) {
      LOG.info("command override disabled");
    } else {
      List<String> overrideCommands = new ArrayList<>();
      Path launchDst =
              new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

      overrideCommands.add("bash");
      overrideCommands.add(launchDst.toUri().getPath());
      runCommand.setOverrideCommandWithArgs(overrideCommands);
    }

    if(enableUserReMapping) {
      runCommand.groupAdd(groups);
    }

    String commandFile = dockerClient.writeCommandToTempFile(runCommand,
            containerIdStr);
    PrivilegedOperation launchOp = buildLaunchOp(ctx,
            commandFile, runCommand);

    LOG.info("junqing xiao : final docker command is : "+runCommand.getCommandWithArguments());


    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
              launchOp, null, null, false, false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Launch container failed. Exception: ", e);
      LOG.info("Docker command used: " + runCommand.getCommandWithArguments());

      throw new ContainerExecutionException("Launch container failed", e
              .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
          throws ContainerExecutionException {
    ContainerExecutor.Signal signal = ctx.getExecutionAttribute(SIGNAL);

    PrivilegedOperation privOp = null;
    // Handle liveliness checks, send null signal to pid
    if(ContainerExecutor.Signal.NULL.equals(signal)) {
      privOp = new PrivilegedOperation(
              PrivilegedOperation.OperationType.SIGNAL_CONTAINER);
      privOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
              ctx.getExecutionAttribute(USER),
              Integer.toString(PrivilegedOperation.RunAsUserCommand
                      .SIGNAL_CONTAINER.getValue()),
              ctx.getExecutionAttribute(PID),
              Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

      // All other signals handled as docker stop
    } else {
      String containerId = ctx.getContainer().getContainerId().toString();
      DockerStopCommand stopCommand = new DockerStopCommand(containerId);
      String commandFile = dockerClient.writeCommandToTempFile(stopCommand,
              containerId);
      privOp = new PrivilegedOperation(
              PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
      privOp.appendArgs(commandFile);
    }

    //Some failures here are acceptable. Let the calling executor decide.
    privOp.disableFailureLogging();

    try {
      privilegedOperationExecutor.executePrivilegedOperation(null,
              privOp , null, null, false, false);
    } catch (PrivilegedOperationException e) {
      throw new ContainerExecutionException("Signal container failed", e
              .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
          throws ContainerExecutionException {
  }


  // ipAndHost[0] contains comma separated list of IPs
  // ipAndHost[1] contains the hostname.
  @Override
  public String[] getIpAndHost(Container container) {
    String containerId = container.getContainerId().toString();
    DockerInspectCommand inspectCommand =
            new DockerInspectCommand(containerId).getIpAndHost();
    try {
      String commandFile = dockerClient.writeCommandToTempFile(inspectCommand,
              containerId);
      PrivilegedOperation privOp = new PrivilegedOperation(
              PrivilegedOperation.OperationType.RUN_DOCKER_CMD);
      privOp.appendArgs(commandFile);
      String output = privilegedOperationExecutor
              .executePrivilegedOperation(null, privOp, null,
                      null, true, false);
      LOG.info("Docker inspect output for " + containerId + ": " + output);
      int index = output.lastIndexOf(',');
      if (index == -1) {
        LOG.error("Incorrect format for ip and host");
        return null;
      }
      String ips = output.substring(0, index).trim();
      String host = output.substring(index+1).trim();
      String[] ipAndHost = new String[2];
      ipAndHost[0] = ips;
      ipAndHost[1] = host;
      return ipAndHost;
    } catch (ContainerExecutionException e) {
      LOG.error("Error when writing command to temp file", e);
    } catch (PrivilegedOperationException e) {
      LOG.error("Error when executing command.", e);
    }
    return null;
  }



  private PrivilegedOperation buildLaunchOp(ContainerRuntimeContext ctx,
                                            String commandFile, DockerRunCommand runCommand) {

    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    String containerIdStr = ctx.getContainer().getContainerId().toString();
    Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
            NM_PRIVATE_CONTAINER_SCRIPT_PATH);
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);
    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    PrivilegedOperation launchOp = new PrivilegedOperation(
            PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER);

    launchOp.appendArgs(runAsUser, ctx.getExecutionAttribute(USER),
            Integer.toString(PrivilegedOperation
                    .RunAsUserCommand.LAUNCH_DOCKER_CONTAINER.getValue()),
            ctx.getExecutionAttribute(APPID),
            containerIdStr,
            containerWorkDir.toString(),
            nmPrivateContainerScriptPath.toUri().getPath(),
            ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
            ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                    localDirs),
            StringUtils.join(PrivilegedOperation.LINUX_FILE_PATH_SEPARATOR,
                    logDirs),
            commandFile,
            resourcesOpts);

    String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

    if (tcCommandFile != null) {
      launchOp.appendArgs(tcCommandFile);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Launching container with cmd: " + runCommand
              .getCommandWithArguments());
    }

    return launchOp;
  }

  public static void validateImageName(String imageName)
          throws ContainerExecutionException {
    if (imageName == null || imageName.isEmpty()) {
      throw new ContainerExecutionException(
              ENV_DOCKER_CONTAINER_IMAGE + " not set!");
    }
    if (!dockerImagePattern.matcher(imageName).matches()) {
      throw new ContainerExecutionException("Image name '" + imageName
              + "' doesn't match docker image name pattern");
    }
  }
}
