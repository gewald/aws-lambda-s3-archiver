package de.zalando.hackweek;

import lombok.Data;

@Data
public class LogEntry {

    private String appId;
    private String simpleSku;
    private String timestamp;
    private String flowId;
    private String nakadiEid;
    private String description;

}
