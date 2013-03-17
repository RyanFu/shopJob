package org.dueam.hadoop.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.dueam.hadoop.utils.uic.UicUtils;

public class HttpUtils {

	/**
	 * @param args
	 * @throws java.io.IOException
	 */
	public static void main(String[] args) throws IOException {
		long begin = System.currentTimeMillis();
		for(int i=0;i<1000;i++){
		StringBuffer content = new StringBuffer();
		URL url = null;
		InputStream is = null;
		try {
			url = new URL(UicUtils.UIC_CONSOLE_CACHE+"1234");
			is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is,"utf-8"));
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
		}

		System.out.println(System.currentTimeMillis() - begin);

	}

	private static String DEFAULT_CHARSET = "utf-8";
	public static String getContent(String inputUrl){
		return getContent(inputUrl, DEFAULT_CHARSET);
	}
	public static String getContent(String inputUrl,String charSet) {
		StringBuffer content = new StringBuffer();
		URL url = null;
		InputStream is = null;
		try {
			url = new URL(inputUrl);
			is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is,charSet));
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
