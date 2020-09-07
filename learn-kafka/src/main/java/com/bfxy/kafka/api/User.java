package com.bfxy.kafka.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @CreateDate: 2020/9/5 09:26 <br>
 * @Author: zhaojh
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private String id;
    private String name;
}
