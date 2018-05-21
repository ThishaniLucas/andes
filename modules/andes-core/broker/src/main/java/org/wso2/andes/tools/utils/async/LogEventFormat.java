package org.wso2.andes.tools.utils.async;

public class LogEventFormat {
    private long timeStamp;
    private String jmsProperties;
    private String user;
    private String jmsMsgId;
    private String destination;
    private TraceMessageStatus status;

    LogEventFormat(long timeStamp, String jmsProperties, String user, String jmsMsgId,  String destination, TraceMessageStatus status) {
        this.timeStamp = timeStamp;
        this.jmsProperties = jmsProperties;
        this.user = user;
        this.jmsMsgId = jmsMsgId;
        this.destination = destination;
        this.status = status;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getJmsProperties() {
        return jmsProperties;
    }

    public String getUser() {
        return user;
    }

    public String getJmsMsgId() {
        return jmsMsgId;
    }

    public String getDestination() {
        return destination;
    }

    public TraceMessageStatus getStatus() {
        return status;
    }
}
