package com.example.dw.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/04 14:57
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserUserLogin {
    // 窗口开始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String curDate;
    // 独立用户数
    private Long uuCt;
    // 货流用户数
    private Long backCt;
    // 时间戳
    @JSONField(serialize = false)
    private Long ts;
}
