package org.dueam.hadoop.services;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * User: windonly
 * Date: 11-3-30 ÉÏÎç11:30
 */
public class CategoryDO {
    private String categoryId;

    public String getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(String categoryId) {
        this.categoryId = categoryId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getRootId() {
        if (null == rootId || "".endsWith(rootId)) return categoryId;
        return rootId;
    }

    public void setRootId(String rootId) {

        this.rootId = rootId;
    }

    public String getRootName() {
        if (null == rootName || "".endsWith(rootName)) return name;
        return rootName;
    }

    public void setRootName(String rootName) {
        this.rootName = rootName;
    }

    private String name;
    private String parentId;
    private String rootId;
    private String rootName;

    public String getCatPath() {
        return catPath;
    }

    public void setCatPath(String catPath) {
        this.catPath = catPath;
    }

    private String catPath;

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    private String status;

    public String toString(){
        return ToStringBuilder.reflectionToString(this);
    }

    public boolean hasRoot(){
        return rootId == null || "".equals(rootId);
    }
}
