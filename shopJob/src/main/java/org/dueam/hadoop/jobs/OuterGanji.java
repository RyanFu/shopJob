package org.dueam.hadoop.jobs;

import org.dueam.hadoop.utils.HttpUtils;

/**
 * User: windonly
 * Date: 11-8-5 обнГ10:19
 */
public class OuterGanji {
    public static void main(String[] args){
         getPage("http://hz.ganji.com/jiaju/");
    }

    public static void getPage(String url){
       String content = HttpUtils.getContent(url);
        System.out.println(content);
    }
}
