package com.example.dw.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/04 16:19
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口开始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String curDate;
    // 加购独立用户数
    private Long cartAddUuCt;
}
