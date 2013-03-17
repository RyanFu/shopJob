package org.dueam.hadoop.services;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.*;

/**
 * User: windonly
 * Date: 11-3-30 ÉÏÎç11:32
 */
public class CategoryLoadService {

    public static void main(String[] args) throws Exception {

    }

    public static Map<String, CategoryDO> loadCategory(String input) {
        Map<String, CategoryDO> catMap = new HashMap<String, CategoryDO>();
        Map<String, CategoryDO> rootMap = new HashMap<String, CategoryDO>();
        try {
            for (String line : (List<String>) FileUtils.readLines(new File(input), "utf-8")) {
                String[] array = StringUtils.splitPreserveAllTokens(line, (char) 0x09);
                CategoryDO category = new CategoryDO();
                category.setCategoryId(StringUtils.trim(array[0]));
                category.setParentId(StringUtils.trim(array[1]));
                category.setName(StringUtils.trim(array[2]));
                catMap.put(array[0], category);
                if ("0".equals(array[1]) || null == array[1]) {
                    rootMap.put(array[0], category);
                }
            }

            for (CategoryDO category : catMap.values()) {
                CategoryDO _tmp = category;
                List<String> catPath = new ArrayList<String>();
                catPath.add(_tmp.getName());
                while (!"0".equals(_tmp.getParentId())) {
                    _tmp = catMap.get(_tmp.getParentId());
                    if (_tmp == null) {
                        break;
                    }
                    catPath.add(_tmp.getName());
                }
                if (_tmp != null) {
                    category.setRootId(_tmp.getCategoryId());
                    category.setRootName(_tmp.getRootName());
                }
                Collections.reverse(catPath);
                category.setCatPath(StringUtils.join(catPath,">>"));
            }
        } catch (Exception e) {

        }
        return catMap;
    }

}
