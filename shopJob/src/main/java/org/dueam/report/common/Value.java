package org.dueam.report.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * User: windonly
 * Date: 11-3-7 ÉÏÎç11:11
 */
public class Value {

    private String key;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private String name;

    protected Value(String key, String name, String value) {
        this.key = key;
        this.name = name;
        this.value = value;
    }

    public List<Value> getKeyDetail() {
        return keyDetail;
    }

    public List<Value> keyDetail = null;

    public void setHasDetail(boolean hasDetail) {
        this.hasDetail = hasDetail;
    }

    private boolean hasDetail = false;

    public boolean isHasDetail() {
        return this.hasDetail;
    }


    public void addDetail(String key, String value) {
        if (keyDetail == null) {
            keyDetail = new ArrayList<Value>();
            hasDetail = true;
        }
        keyDetail.add(new Value(key, key, value));
    }

    public final static String SORT_TYPE_TIME = "time";

    public void sortDetail(String type) {
        if (keyDetail == null || keyDetail.isEmpty()) return;
        if (SORT_TYPE_TIME.equals(type)) {
            Collections.sort(keyDetail, new Comparator<Value>() {
                public int compare(Value o1, Value o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });
        }
    }

    protected Value(String key, String name, String value, String type) {
        this.key = key;
        this.name = name;
        this.value = value;
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        if (RUtils.isEmpty(value)) {
            return "0";
        }
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isEmptyKey() {
        return RUtils.isEmpty(this.key);
    }

    private String value;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getValueAsLong() {
        return RUtils.toLong(this.value, 0);
    }

    private String type;

    public static final String BREAK_KEY = "break_key";

    public boolean isBreak() {
        return BREAK_KEY.equals(this.key);
    }

    public static final String TYPE_MONEY = "money";
    public static final String TYPE_TIME = "time";
    public static final String TYPE_NUMBER = "num";
    public static final String TYPE_SHOW = "show";

    public boolean isShow(){
        return TYPE_SHOW.equals(type);
    }
}