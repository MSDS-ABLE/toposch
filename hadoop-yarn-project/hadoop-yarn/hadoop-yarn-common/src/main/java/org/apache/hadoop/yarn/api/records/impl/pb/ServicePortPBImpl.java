package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ServicePort;

import org.apache.hadoop.yarn.proto.YarnProtos.ServicePortProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ServicePortProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ZJY on 1/19/18.
 */
public class ServicePortPBImpl extends ServicePort{
    ServicePortProto proto = ServicePortProto.getDefaultInstance();
    ServicePortProto.Builder builder = null;
    private static final Logger LOG =
            LoggerFactory.getLogger(ServicePortPBImpl.class);

    boolean viaProto = false;

    // call via ProtoUtils.convertToProtoFormat(Resource)
    static ServicePortProto getProto(ServicePort SP) {
        final ServicePortPBImpl pb;
        pb = new ServicePortPBImpl();
        pb.setServicePort(SP.getPort());
        return pb.getProto();
    }

    public ServicePortPBImpl() {
        builder = ServicePortProto.newBuilder();
    }

    public ServicePortPBImpl(ServicePortProto proto) {
        this.proto = proto;
        viaProto = true;
    }
    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            builder = ServicePortProto.newBuilder(proto);
        }
        viaProto = false;
    }
    public int getServicePort() {
        ServicePortProtoOrBuilder p = viaProto ? proto : builder;
        return p.getPort();
    }


    @Override
    public int getPort() {
        return (int)getServicePort();
    }

    @Override
    public void setPort(int port) {
        setServicePort(port);
    }

    public void setServicePort(int port) {
        maybeInitBuilder();
        builder.setPort(port);
    }

    public ServicePortProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    private void mergeLocalToProto() {
        if (viaProto) {
            maybeInitBuilder();
        }
        builder.setPort(port);
        //mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;

    }
}
