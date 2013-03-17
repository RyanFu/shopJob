package org.dueam.report.common;

import org.apache.commons.lang.math.NumberUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * User: windonly
 * Date: 11-3-7 ÉÏÎç11:10
 */
public class Table {
    public final static String GROUP_TYPE = "group";

    public final static String VIEW_TYPE = "view";

    public boolean isViewTable() {
        return VIEW_TYPE.equals(type);
    }

    public boolean isGroupTable() {
        return GROUP_TYPE.equals(type);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    private String type = "1";

    protected Table(String id, String summary, String keyTitle, String valueTitle) {
        this.id = id;
        this.summary = summary;
        this.keyTitle = keyTitle;
        this.valueTitle = valueTitle;
    }

    public void addCol(Value value) {
        this.values.add(value);
    }

    public void breakRow() {
        this.values.add(Report.BREAK_VALUE);
    }

    public Table addCol(String value) {
        this.values.add(new Value(null, null, value));
        return this;
    }

    public void addCol(String key, String value) {
        this.values.add(Report.newValue(key, value));
    }

    public void addCol(String key, String keyName, String value) {
        this.values.add(Report.newValue(key, keyName, value));
    }

    public void addTimeCol(String key, String keyName, String value) {
        this.values.add(Report.newTimeValue(key, keyName, value));
    }

    public void addMoneyCol(String key, String keyName, String value) {
        this.values.add(Report.newMoneyValue(key, keyName, value));
    }

    public void addShowCol(String key, String keyName, String value) {
        this.values.add(Report.newMoneyValue(key, keyName, value));
    }


    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public List<Value> getValues() {
        return values;
    }

    public void setValues(List<Value> values) {
        this.values = values;
    }

    private String summary;
    private String keyTitle;

    public String getKeyTitle() {
        return keyTitle;
    }

    public void setKeyTitle(String keyTitle) {
        this.keyTitle = keyTitle;
    }

    public String getValueTitle() {
        return valueTitle;
    }

    public void setValueTitle(String valueTitle) {
        this.valueTitle = valueTitle;
    }

    private String valueTitle;
    private List<Value> values = new ArrayList<Value>();
    public final static int SORT_VALUE = 2;
    public final static int SORT_KEY = 1;
    public final static int SORT_KEY_VALUE = 3;

    public List<List<Value>> getViewValues() {
        List<List<Value>> list = new ArrayList<List<Value>>();
        List<Value> cols = new ArrayList<Value>();
        for (Value value : values) {
            if (value.isBreak()) {
                list.add(cols);
                cols = new ArrayList<Value>();
            } else {
                cols.add(value);
            }
        }
        if (!cols.isEmpty()) {
            list.add(cols);
        }
        return list;
    }

    public void sort(int type) {
        switch (type) {
            case SORT_VALUE:
                Collections.sort(values, new Comparator<Value>() {
                    public int compare(Value o1, Value o2) {
                        long v1 = NumberUtils.toLong(o1.getValue());
                        long v2 = NumberUtils.toLong(o2.getValue());
                        if (v1 == v2) return 0;
                        return v1 < v2 ? 1 : -1;
                    }
                });
                break;
            case SORT_KEY:
                Collections.sort(values, new Comparator<Value>() {
                    public int compare(Value o1, Value o2) {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                });
                break;
        }
    }

    public String getKeyName(String key) {
        for (Value value : values) {
            if (RUtils.equals(value.getKey(), key)) {
                return value.getName();
            }
        }
        return null;
    }

    private long sumCache = -1;

    public long getSum() {
        long _sum = 0;
        if (this.values != null) {
            for (Value value : this.values) {
                _sum += RUtils.toLong(value.getValue(), 0);
            }
        }
        return _sum;

    }

    public long getMax() {
        long max = 0;
        if (this.values != null) {
            for (Value value : this.values) {
                long v = RUtils.toLong(value.getValue(), 0);
                if (max < v) {
                    max = v;
                }
            }
        }
        return max;

    }

    public long getSumCache() {
        if (sumCache < 0) {
            this.sumCache = this.getSum();
        }

        return this.sumCache;
    }

    public int getColSize() {
        int size = 0;
        for (Value value : values) {
            if (value.isBreak()) {
                return size;
            }
            size++;
        }
        return size;
    }

    public double sumRow(int row) {
        double sum = 0;
        for (int i = row; i < values.size(); i = i + getColSize() + 1) {
            sum += NumberUtils.toDouble(values.get(i).getValue());
        }
        return sum;
    }

}





