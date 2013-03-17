package org.dueam.hadoop.services;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * User: windonly
 * Date: 11-8-9 ÏÂÎç5:53
 */
public class ShopDomain {
    static boolean init = false;
    static Set<String> domains = null;
    private final static String DOMAIN_PATH = "/home/suoni/bp/services/domain";

    public static void main(String[] args){
        System.out.println(ShopDomain.isShopDomain("lining"));
    }


    public static boolean isShopDomain(String domain) {
        init();
        return domains.contains(StringUtils.lowerCase(domain));
    }

    private static void init() {
        if (!init) {
            synchronized (ShopDomain.class) {
                if (!init) {
                    domains = new HashSet<String>();
                    File input = new File(DOMAIN_PATH);
                    if (!input.exists()) {
                        input = new File("domain");
                    }
                    InputStream in = null;
                    try {
                        in = FileUtils.openInputStream(input);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                        String line = reader.readLine();
                        while (line != null) {
                            domains.add(StringUtils.lowerCase(StringUtils.trim(line)));
                            line = reader.readLine();
                        }
                    } catch (Exception e) {

                    } finally {
                        IOUtils.closeQuietly(in);
                    }

                    init = true;
                }
            }
        }
    }
}
