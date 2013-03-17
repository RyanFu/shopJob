package org.dueam.services;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

/**
 * 加载URL上面的内容
 * User: windonly
 * Date: 11-6-27 下午4:45
 */
public abstract class HttpUtils {
    private static String DEFAULT_CHARSET = "utf-8";

    /**
     * 加载Url里的内容（默认编码：utf-8）
     * @param inputUrl url地址
     * @return
     */
    public static String getContent(String inputUrl) {
        return getContent(inputUrl, DEFAULT_CHARSET);
    }

    /**
     * 加载Url里面的内容
     * @param inputUrl  URL地址
     * @param charSet   编码
     * @return
     */
    public static String getContent(String inputUrl, String charSet) {
        StringBuffer content = new StringBuffer();
        URL url = null;
        InputStream is = null;
        try {
            url = new URL(inputUrl);
            is = url.openStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is, charSet));
            String line;
            while ((line = br.readLine()) != null)
                content.append(line);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (is != null)
                    is.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return content.toString();
    }

}
