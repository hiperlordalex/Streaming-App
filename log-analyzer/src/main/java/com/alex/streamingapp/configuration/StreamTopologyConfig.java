package com.alex.streamingapp.configuration;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Valente on 2017.04.30..
 */
public final class StreamTopologyConfig implements Serializable {
    private final Logger LOG = LoggerFactory.getLogger(StreamTopologyConfig.class);

    private static final String CONFIG_FILE = "conf.json";
    private JsonObject jsonObject;
    private JsonParser parser = new JsonParser();

    private static StreamTopologyConfig INSTANCE = new StreamTopologyConfig();

    private StreamTopologyConfig() {
        try {
            System.out.println(Resources.toString(Resources.getResource(CONFIG_FILE), Charsets.UTF_8));
            jsonObject = parser.parse(Resources.toString(Resources.getResource(CONFIG_FILE), Charsets.UTF_8)).getAsJsonObject();
            LOG.info(jsonObject.toString());
        } catch (FileNotFoundException e) {
            LOG.error(e.getLocalizedMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    public static StreamTopologyConfig getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new StreamTopologyConfig();
        }
        return INSTANCE;
    }

    // ZooKeeper
    public JsonObject getZookeeper() {
        return jsonObject.getAsJsonObject("zookeeper");
    }

    public String getZkHosts() {
        return getZookeeper().get("zk_host").getAsString();
    }

    public int getZkPort() {
        return getZookeeper().get("zk_port").getAsInt();
    }


    // Kafka
    public JsonObject getKafkaTopics() {
        return jsonObject.getAsJsonObject("kafka_topics");
    }

    public String getKafkaTopicJMXTrans() {
        return getKafkaTopics().get("kafka_topic_jmxtrans").getAsString();
    }

    public String getKafkaTopicLogs() {
        return getKafkaTopics().get("kafka_topic_logs").getAsString();
    }

}
