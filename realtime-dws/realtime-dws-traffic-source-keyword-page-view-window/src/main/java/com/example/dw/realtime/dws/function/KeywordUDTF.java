package com.example.dw.realtime.dws.function;

import com.example.dw.realtime.dws.util.KeywordUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Description: 自定义UDTF
 * @Author: Chenyang on 2024/12/03 9:48
 * @Version: 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String text) {
        for (String keyword : KeywordUtils.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
