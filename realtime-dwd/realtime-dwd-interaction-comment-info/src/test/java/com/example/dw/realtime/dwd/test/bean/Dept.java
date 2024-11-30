package com.example.dw.realtime.dwd.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/29 20:43
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    private Integer deptNo;
    private String deptName;
    Long ts;
}
