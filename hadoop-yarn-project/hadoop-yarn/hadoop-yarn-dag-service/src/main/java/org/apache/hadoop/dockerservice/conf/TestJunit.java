package org.apache.hadoop.dockerservice.conf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;


/**
 * Created by root on 1/17/18.
 */
public class TestJunit {
    public static final Log LOG = LogFactory.getLog(TestJunit.class);

    @Test
    public void hello(){
        LOG.info("hello jnuit");
    }
}
