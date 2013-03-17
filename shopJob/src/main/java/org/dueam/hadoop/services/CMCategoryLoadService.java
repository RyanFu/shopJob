package org.dueam.hadoop.services;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.dueam.hadoop.common.tables.CmCategories;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-4-22 ÉÏÎç10:33
 */
public class CMCategoryLoadService {
    public static void main(String[] args) throws Exception {
        loadCategory("d:\\user_view_ip\\cm_cat", "1");
    }

    public static Map<String, CategoryDO> loadCategory(String input, String rootId) {
        Map<String, CategoryDO> catMap = new HashMap<String, CategoryDO>();
        Map<String, CategoryDO> rootMap = new HashMap<String, CategoryDO>();
        try {
            for (String line : (List<String>) FileUtils.readLines(new File(input), "utf-8")) {

                String[] array = StringUtils.splitPreserveAllTokens(line, (char) 0x09);
                String catId = array[CmCategories.cat_id];
                String parentId = array[CmCategories.parent_id];
                String catName = array[CmCategories.cat_name];
                String conditions = array[CmCategories.conditions];
                String status = array[CmCategories.status];
                String catType = array[CmCategories.cat_type];
                if ((catType.equals("1") || catType.equals("2")) && "0".equals(status)) {
                    if (conditions.indexOf('S') >= 0) {
                        System.out.println(line);
                    }
                }
                if (!"0".equals(status) || !rootId.equals(catType)) continue;

                CMCategoryDO category = new CMCategoryDO();
                category.setCategoryId(catId);
                category.setParentId(parentId);
                category.setName(catName);
                category.setStatus(status);
                category.setConditions(conditions);
                catMap.put(catId, category);
                if (rootId.equals(parentId) || null == parentId) {
                    rootMap.put(array[0], category);
                }
            }

            for (CategoryDO category : catMap.values()) {
                CategoryDO _tmp = category;
                while (!rootId.equals(_tmp.getParentId())) {
                    _tmp = catMap.get(_tmp.getParentId());
                    if (_tmp == null) {
                        break;
                    }
                }
                if (_tmp != null) {
                    category.setRootId(_tmp.getCategoryId());
                    category.setRootName(_tmp.getRootName());
                }
            }
        } catch (Exception e) {

        }
        return catMap;
    }
}
