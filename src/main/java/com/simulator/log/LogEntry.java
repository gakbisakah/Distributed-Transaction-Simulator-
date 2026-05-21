package com.simulator.log;

/**
 * Entri log untuk Write-Ahead Log
 */
public class LogEntry {
    private long lsn;
    private final String transactionId;
    private final LogType logType;
    private final String data;
    private long timestamp;
    
    public LogEntry(String transactionId, LogType logType, String data) {
        this.transactionId = transactionId;
        this.logType = logType;
        this.data = data;
    }
    
    public long getLsn() {
        return lsn;
    }
    
    public void setLsn(long lsn) {
        this.lsn = lsn;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public LogType getLogType() {
        return logType;
    }
    
    public String getData() {
        return data;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return String.format("LogEntry{lsn=%d, tx='%s', type=%s, timestamp=%d}",
                           lsn, transactionId, logType, timestamp);
    }
}