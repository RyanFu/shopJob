package org.dueam.hadoop.common.util;

import java.util.*;

/**
 * simple report
 * User: windonly
 * Date: 10-12-20 ÏÂÎç5:09
 */
public class Report {
    /**
     * a new table of report
     *
     * @param name table name
     * @param desc a desc of table
     * @return
     */
    public static Table newTable(String name, String desc, String id) {
        Table table = new Table(name, desc, id);
        tableMap.put(name, table);
        lastTable = table;
        return table;
    }

    public static Table newTable(String name) {
        return newTable(name, null, null);
    }

    private static Table lastTable;

    public static Table add(String... cols) {
        if (lastTable == null) {
            throw new NullPointerException("Please new a table first !");
        }
        lastTable.add(cols);
        return lastTable;
    }

    private static Map<String, Table> tableMap = new LinkedHashMap<String, Table>();

    /**
     * render a html report
     *
     * @return
     */
    public static String renderHtml(String commonHtml) {
        StringBuffer html = new StringBuffer();
        html.append(HtmlReport.CSS_TABLE);
        if (commonHtml != null) html.append(commonHtml);
        for (Table table : tableMap.values()) {
            html.append("<p>").append(table.getName()).append("</p>");
            html.append("<table id='").append(table.getId()).append("'>\n");
            for (String[] cols : table.getTable()) {
                html.append("<tr>\n");
                for (String col : cols) {
                    html.append("<td>").append(col).append("</td>\n");
                }
                html.append("</tr>\n");
            }
            html.append("</table>\n");
        }
        return html.toString();
    }

    public static class Table {
        public String getName() {
            return name;
        }

        public String getDesc() {
            return desc;
        }

        private String name;
        private String desc;

        public String getId() {
            return id;
        }

        private String id;

        public List<String[]> getTable() {
            return table;
        }

        private List<String[]> table = new ArrayList<String[]>();

        public Table(String name, String desc, String id) {
            this.name = name;
            this.desc = desc;
            this.id = id;
        }

        public Table add(String[] cols) {
            table.add(cols);
            return this;
        }

    }
}
