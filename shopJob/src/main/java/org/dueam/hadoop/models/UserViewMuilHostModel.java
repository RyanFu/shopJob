package org.dueam.hadoop.models;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * User: windonly
 * Date: 11-4-8 下午1:44
 */
public class UserViewMuilHostModel {
   final static char TAB = (char) 0x09;
    public static void  main(String[] args) throws Exception{
        System.out.println("20110407123456".substring(0,12)+"00");
       List<String[]> _temp = new ArrayList<String[]>();
        List<String>lines = FileUtils.readLines(new File("sample"),"gbk");
        for(String line : lines){
            String[] tokens = StringUtils.splitPreserveAllTokens(line, TAB);
            _temp.add(tokens);
        }

        /**
         * 分组算法
         * FOREACH ALL_DATA
         *  IF IN INDEX THEN
         *    UPDATE INDEX AND INSERT DATA
         *  ELSE
         *    FOREACH SUB_DATA
         *      MAKE INDEX AND SET FIND'S DATA AS NULL
         */
        List<List<String[]>> dataList = new ArrayList<List<String[]>>();
        List<StringBuffer> indexList = new ArrayList<StringBuffer>();

        for (int posI = 0; posI < _temp.size(); posI++) {
            String[] array = _temp.get(posI);
            if (array == null) continue;

            String mid = array[2];
            String ip = array[3];
            boolean hasIndex = false;
            for (int i = 0; i < indexList.size(); i++) {
                StringBuffer index = indexList.get(i);
                if (index.indexOf("|" + mid + "|") >= 0 || index.indexOf("|" + ip + "|") >= 0) {
                    if (index.indexOf("|" + mid + "|") < 0) {
                        index.append('|').append(mid).append('|');
                    }

                    if (index.indexOf("|" + ip + "|") < 0) {
                        index.append('|').append(ip).append('|');
                    }
                    dataList.get(i).add(array);
                    hasIndex = true;
                    break;
                }

            }
            if (!hasIndex) {
                StringBuffer index = new StringBuffer("|" + mid + "|" + ip + "|");
                List<String[]> _tmp = new ArrayList<String[]>();
                _tmp.add(array);
                for (int k = posI + 1; k < _temp.size(); k++) {
                    String[] _newArray = _temp.get(k);
                    if (_newArray == null) {
                        continue;
                    }
                    String _mid = _newArray[2];
                    String _ip = _newArray[3];
                    if (index.indexOf("|" + _mid + "|") >= 0 || index.indexOf("|" + _ip + "|") >= 0) {
                        if (index.indexOf("|" + _mid + "|") < 0) {
                            index.append('|').append(_mid).append('|');
                        }

                        if (index.indexOf("|" + _ip + "|") < 0) {
                            index.append('|').append(_ip).append('|');
                        }
                        _tmp.add(_newArray);
                        _temp.set(k,null);
                    }
                }
                indexList.add(index);
                dataList.add(_tmp);
            }
        }

        System.out.println(dataList.get(0).size()+ dataList.get(1).size());
    }
}
