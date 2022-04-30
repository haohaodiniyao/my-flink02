package com.example;

import com.example.file.MyJob;

public class FlinkApplication {
    private static final String JOB_NAME = "my flink";

    public static void main(String[] args) throws Exception {
        MyJob myJob = new MyJob(JOB_NAME, args);
        myJob.execute();
    }
}
