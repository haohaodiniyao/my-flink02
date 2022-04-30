package com.example.file;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
@SuppressWarnings("all")
@ToString
@Data
@Slf4j
public class MsgData implements Serializable {
    private static final long serialVersionUID = 1L;
    private Long ts;
    private String msg;
    private String dt;
}
