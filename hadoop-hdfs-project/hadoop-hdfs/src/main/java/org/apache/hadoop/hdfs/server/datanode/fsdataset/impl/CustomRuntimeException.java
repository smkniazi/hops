package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

public class CustomRuntimeException extends RuntimeException {

    public String message;

    public CustomRuntimeException(String message){
        this.message = message;
    }

    // Overrides Exception's getMessage()
    @Override
    public String getMessage(){
        return message;
    }
}
