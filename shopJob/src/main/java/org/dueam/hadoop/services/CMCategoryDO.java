package org.dueam.hadoop.services;

/**
 * User: windonly
 * Date: 11-4-22 ионГ10:46
 */
public class CMCategoryDO extends CategoryDO{
    public String getConditions() {
        return conditions;
    }

    public void setConditions(String conditions) {
        this.conditions = conditions;
    }

    private String conditions ;
}
