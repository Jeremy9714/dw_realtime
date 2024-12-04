package com.example.dw.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description:
 * @Author: Chenyang on 2024/12/04 13:53
 * @Version: 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrafficHomeDetailPageView {
    // 窗口起始时间
    private String stt;
    // 窗口结束时间
    private String edt;
    // 当天日期
    private String curDate;
    // 首页独立访问数
    private Long homeUvCt;
    // 商品详情页独立访问数
    private Long goodDetailUvCt;
    // 时间戳
    @JSONField(serialize = false)
    private long ts;
}
