import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: windonly
 * Date: 11-12-20
 * Time: ����8:42
 * To change this template use File | Settings | File Templates.
 */
public class ListMarket {
    public static void main(String[] args) throws Exception {
        List<String> input = (List<String>) FileUtils.readLines(new File("/Users/windonly/Dropbox/home/tb/hadoop/input.csv"));
        Map<String,String> markMap = new HashMap<String, String>();
        for(String[] array : mark){
            markMap.put(array[0],array[1]);
        }

        List<String> result = new ArrayList<String>();
        result.add("\"�г�����\","+input.get(0));
        for (int i=1;i<input.size();i++) {
            String line = input.get(i);
            String[] cols = StringUtils.split(line,',');
            String id = StringUtils.remove(cols[1],'"');
            result.add("\""+markMap.get(id)+"\","+line);
        }

        FileUtils.writeLines(new File("/Users/windonly/Dropbox/home/tb/hadoop/pv.csv"),"GBK",result);
    }

    public static String[][] mark = {{"0", "���г�"},
            {"869", "ĸӤ�г�"},
            {"874", "Ůװ�г�"},
            {"872", "�����г�"},
            {"543", "�鱦�г�"},
            {"544", "�����г�"},

            {"546", "�˶��г�"},
            {"548", "�ٻ��г�"},
            {"861", "�Ҿ��г�"},
            {"843", "�����г�"},
            {"551","��Ʒ�г�"},
            {"552", "�ֱ��г�"},
            {"895", "��װ�г�"},
            {"568", "����г�"},
            {"569", "������Ʒ�г�"},
            {"571", "�ҷ��г�"},
            {"898", "�����г�"},
            {"576", "�����г�"},
            {"578", "�����г�"},
            {"581", "�۾��г�"},
            {"582", "��ȯ�г�"},
            {"808", "�����г�"},
            {"835", "�Ա�����"},
            {"894", "Ů���г�"},
            {"897", "�а��г�"},
            {"899", "�����г�"}};
}
