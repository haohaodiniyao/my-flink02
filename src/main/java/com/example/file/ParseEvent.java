package com.example.file;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ParseEvent extends RichMapFunction<String,MsgData> {

        @Override
        public MsgData map(String s) throws Exception {
            MsgData msgData = JSON.parseObject(s, MsgData.class);
            String dt = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("Asia/Shanghai")).format(Instant.ofEpochMilli(msgData.getTs()));
            msgData.setDt(dt);
            return msgData;
        }
    }