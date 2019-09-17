package com.itcanteen.es.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author baimugudu
 * @email 2415621370@qq.com
 * @date 2019/9/15 15:07
 */

@Configuration
public class MyConfig {

@Bean
    public TransportClient client(){
        InetSocketTransportAddress node = null;
        try {
             node = new InetSocketTransportAddress(
                    InetAddress.getByName("10.211.55.8"),
                    9300
            );
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        Settings build = Settings.builder()
                .put("cluster.name", "aisile")
                .put("node.name", "master")
                .build();

        PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(build);
        preBuiltTransportClient.addTransportAddress(node);
        return preBuiltTransportClient;

    }
}
