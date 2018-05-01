package org.wso2.andes.tools.utils.async;

public class LogEventFormat {
    private long timeStamp;
    private String msgId;
    private String state;

    LogEventFormat(long timeStamp, String msgId, String state) {
        this.timeStamp = timeStamp;
        this.msgId = msgId;
        this.state = state;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public String getMsgId() {
        return msgId;
    }

    public String getState() {
        return state;
    }
}
