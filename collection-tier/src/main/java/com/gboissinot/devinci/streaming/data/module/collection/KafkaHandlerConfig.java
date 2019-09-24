package com.gboissinot.devinci.streaming.data.module.collection;

import java.util.List;

class KafkaHandlerConfig {

    private boolean active;

    private String topicName;

    private List<String> bootstrapServers;

    public boolean isActive() {
        return active;
    }

    void setActive(boolean active) {
        this.active = active;
    }

    String getTopicName() {
        return topicName;
    }

    void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
