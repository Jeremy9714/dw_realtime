package com.example.dw.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 分词工具类
 * @Author: Chenyang on 2024/12/03 9:26
 * @Version: 1.0
 */
public class KeywordUtils {

    /**
     * 分词
     *
     * @param text
     * @return
     */
    public static List<String> analyze(String text) {
        List<String> keywordList = new ArrayList<>();
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader, true);

        try {
            Lexeme lexeme;
            while ((lexeme = ik.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

    public static void main(String[] args) {
        System.out.println(analyze("小米手机京东自营5G移动联通电信"));
    }
}
