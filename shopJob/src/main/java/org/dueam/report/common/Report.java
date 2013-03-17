package org.dueam.report.common;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-2-15 上午11:29
 */
public class Report {
    public Report() {
    }

    private final static char KEY_SPLIT = 0x02;

    /**
     * 创建索引加快趋势访问速度
     */
    public Map<String, String> makeIndex() {
        Map<String, String> map = new HashMap<String, String>();
        if (this.tables != null) {
            for (Table table : this.tables.values()) {
                for (Value value : table.getValues()) {
                    if (RUtils.isNotEmpty(value.getKey())) {
                        map.put(table.getId() + KEY_SPLIT + value.getKey(), value.getValue());
                    }
                }
            }
        }
        return map;
    }


    public static String mergeKey(String tableId, String key) {
        return tableId + KEY_SPLIT + key;
    }

    protected Map<String, String> index = null;

    public String queryValue(String tableId, String key) {
        if (index == null) {
            index = makeIndex();
        }
        return index.get(tableId + KEY_SPLIT + key);
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    private String title;

    protected Report(String title) {
        this.title = title;
    }

    public static Report newReport(String title) {
        return new Report(title);
    }

    public boolean isEmpty() {
        return this.tables == null || this.tables.isEmpty();
    }

    public Table newTable(String id, String summary, String keyTitle, String valueTitle) {
        if (tables == null) {
            tables = new LinkedHashMap<String, Table>();
        }
        Table table = new Table(id, summary, keyTitle, valueTitle);
        tables.put(id, table);
        return table;
    }

    public Table putTable(Table table) {
        if (tables == null) {
            tables = new LinkedHashMap<String, Table>();
        }
        tables.put(table.getId(), table);
        return table;
    }

    /**
     * group table 会计算每个维度的占比
     *
     * @param id
     * @param summary
     * @param keyTitle
     * @param valueTitle
     * @return
     */
    public Table newGroupTable(String id, String summary, String keyTitle, String valueTitle) {
        Table table = this.newTable(id, summary, keyTitle, valueTitle);
        table.setType(Table.GROUP_TYPE);
        return table;
    }

    public Table newGroupTable(String id, String summary) {
        Table table = this.newTable(id, summary);
        table.setType(Table.GROUP_TYPE);
        return table;
    }

    public static Value BREAK_VALUE = newValue(Value.BREAK_KEY,"-");

    public Table newViewTable(String id, String summary) {
        Table table = this.newTable(id, summary);
        table.setType(Table.VIEW_TYPE);
        return table;
    }

    public Table newTable(String id, String summary) {
        return newTable(id, summary, "纬度", "数量");
    }

    public void addCols(String id, Value value) {
        tables.get(id).addCol(value);
    }

    public Table getTable(String id) {
        return tables.get(id);
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Map<String, Table> getTables() {
        return tables;
    }

    public void setTables(Map<String, Table> tables) {
        this.tables = tables;
    }

    private String summary;

    private Map<String, Table> tables;

    public static Value newBreakValue() {
        return new Value(Value.BREAK_KEY, Value.BREAK_KEY, Value.BREAK_KEY);
    }
    public static Value newValue(String key, String keyName, String value) {
        return new Value(key, keyName, value, Value.TYPE_NUMBER);
    }

    public static Value newTimeValue(String key, String keyName, String value) {
        return new Value(key, keyName, value, Value.TYPE_TIME);
    }

    public static Value newMoneyValue(String key, String keyName, String value) {
        return new Value(key, keyName, value, Value.TYPE_MONEY);
    }

    public static Value newShowValue(String key, String keyName, String value) {
        return new Value(key, keyName, value, Value.TYPE_SHOW);
    }

    public static Value newValue(String key, String value) {
        return new Value(key, key, value);
    }

}
