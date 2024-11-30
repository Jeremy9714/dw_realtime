package com.example.dw.realtime.dwd.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/11/29 20:42
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
    private Integer empNo;
    private String empName;
    private Integer deptNo;
    Long ts;
}
