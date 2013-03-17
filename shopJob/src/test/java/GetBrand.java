import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.dueam.report.common.Value;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * User: windonly
 * Date: 11-5-26 ÉÏÎç9:11
 */
public class GetBrand {
    public static void main(String[] args) throws Exception {
        List<String> list = FileUtils.readLines(new File("brands"));
        List<String> brands = new ArrayList<String>();
        for (String line : list) {
            int pos = line.indexOf("20000:");
            if (pos < 0) continue;
            int nextPos = line.indexOf(';', pos);
            String value = null;
            if (nextPos < 0) {
                value = line.substring(pos + 6);
            } else {
                value = line.substring(pos + 6, nextPos);
            }
            System.out.println(line + "=>" + value);
            brands.add(value);
            //break;
        }
        FileUtils.writeLines(new File("brand_ids"),brands);
    }
}
