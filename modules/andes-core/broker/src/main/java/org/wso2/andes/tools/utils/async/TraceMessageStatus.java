package org.wso2.andes.tools.utils.async;

public enum TraceMessageStatus {
    PUBLISHED,
    DELIVERED,
    ACKNOWLEDGED,
    COMMITED,
    ROLLEDBACK,
    DELETED,
    REDELIVERED,
    REQUEUED
}