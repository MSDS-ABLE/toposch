package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.metrics2.util.MetricsCache;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ZJY on 1/18/18.
 */
public abstract class ServicePort {

    public int port;

    public static ServicePort newInstance(int Port) {
        ServicePort servicePort = Records.newRecord(ServicePort.class);
        servicePort.port = Port;
        return servicePort;
    }

    public static class simplePort extends ServicePort{

        public simplePort (int port){
            this.port = port;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        @Override
        public void setServicePort(int port) {
            this.port = port;
        }

        @Override
        public int getServicePort() {
            return this.port;
        }

    }
    public abstract int getPort();
    public abstract void setPort(int port);
    public abstract void setServicePort(int port);
    public abstract int getServicePort();
}
