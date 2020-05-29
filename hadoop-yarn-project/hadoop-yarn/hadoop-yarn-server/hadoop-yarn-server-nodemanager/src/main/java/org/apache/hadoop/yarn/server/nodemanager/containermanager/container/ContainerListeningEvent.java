package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;

/**
 * Created by ZJY on 1/16/18.
 */
public class ContainerListeningEvent extends ContainerEvent {
    public ContainerListeningEvent(ContainerId c) {
        super(c, ContainerEventType.CONTAINER_RUNNING_LISTENING);
    }
}
