package org.dueam.report.common;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * User: windonly
 * Date: 11-3-7 上午11:37
 */
public class XmlReportFactory {
    public static void dump(Report report, OutputStream outputStream) throws IOException {
        Document document = DocumentHelper.createDocument();
        document.setXMLEncoding("utf-8");
        Element root = document.addElement("report");
        root.addAttribute("title", report.getTitle());
        if (RUtils.isNotEmpty(report.getSummary()))
            root.addElement("summary").setText(report.getSummary());

        if (report.getTables() != null) {
            Element tables = root.addElement("tables");
            for (Table table : report.getTables().values()) {
                Element tableElement = tables.addElement("table");
                tableElement.addAttribute("id", table.getId());
                tableElement.addAttribute("type", table.getType());
                if (RUtils.isNotEmpty(table.getSummary()))
                    tableElement.addElement("summary").setText(table.getSummary());
                if (null != table.getKeyTitle()) {
                    tableElement.addElement("keyTitle").setText(table.getKeyTitle());
                }

                if (null != table.getValueTitle()) {
                    tableElement.addElement("valueTitle").setText(table.getValueTitle());
                }


                if (null != table.getValues()) {
                    Element values = tableElement.addElement("values");
                    for (Value tableValue : table.getValues()) {
                        Element value = values.addElement("value");
                        value.addAttribute("key", tableValue.getKey());
                        value.addAttribute("name", tableValue.getName());
                        value.addAttribute("type", tableValue.getType());
                        if (tableValue.isHasDetail()) {
                            value.addAttribute("detail", "true");
                            throw new NullPointerException("有详情的报表请使用,dump(Report report, OutputStream outputStream, String name, File baseDir) 方法！");
                        }
                        value.setText(tableValue.getValue());
                    }

                }
            }
        }

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        XMLWriter writer = new XMLWriter(outputStream, format);
        writer.write(document);
        writer.close();
    }

    public static void dump(Report report, OutputStream outputStream, String name, File baseDir) throws IOException {
        Document document = DocumentHelper.createDocument();
        document.setXMLEncoding("utf-8");
        Element root = document.addElement("report");
        root.addAttribute("title", report.getTitle());
        if (RUtils.isNotEmpty(report.getSummary()))
            root.addElement("summary").setText(report.getSummary());

        if (report.getTables() != null) {
            Element tables = root.addElement("tables");
            for (Table table : report.getTables().values()) {
                Element tableElement = tables.addElement("table");
                tableElement.addAttribute("id", table.getId());
                tableElement.addAttribute("type", table.getType());
                if (RUtils.isNotEmpty(table.getSummary()))
                    tableElement.addElement("summary").setText(table.getSummary());
                if (null != table.getKeyTitle()) {
                    tableElement.addElement("keyTitle").setText(table.getKeyTitle());
                }

                if (null != table.getValueTitle()) {
                    tableElement.addElement("valueTitle").setText(table.getValueTitle());
                }


                if (null != table.getValues()) {
                    Element values = tableElement.addElement("values");
                    for (Value tableValue : table.getValues()) {
                        Element value = values.addElement("value");
                        value.addAttribute("key", tableValue.getKey());
                        value.addAttribute("name", tableValue.getName());
                        value.addAttribute("type", tableValue.getType());
                        if (tableValue.isHasDetail()) {
                            value.addAttribute("detail", "true");
                            File detailFile = null;
                            if (baseDir != null) {
                                File tmpDir = new File(baseDir, "detail_" + name);
                                if (!tmpDir.exists()) {
                                    tmpDir.mkdirs();
                                }
                                detailFile = new File(baseDir, "detail_" + name + File.separatorChar + name + "_" + table.getId() + "_" + tableValue.getKey() + ".xml");
                            } else {
                                File tmpDir = new File("detail_" + name);
                                if (!tmpDir.exists()) {
                                    tmpDir.mkdirs();
                                }
                                detailFile = new File("detail_" + name + File.separatorChar + name + "_" + table.getId() + "_" + tableValue.getKey() + ".xml");
                            }
                            dumpDetail(tableValue.getKeyDetail(), new FileOutputStream(detailFile));
                        }
                        value.setText(tableValue.getValue());
                    }

                }
            }
        }

        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        XMLWriter writer = new XMLWriter(outputStream, format);
        writer.write(document);
        writer.close();
    }

    public static void dumpDetail(List<Value> details, OutputStream outputStream) throws IOException {
        Document document = DocumentHelper.createDocument();
        document.setXMLEncoding("utf-8");
        Element root = document.addElement("details");
        for (Value tableValue : details) {
            Element value = root.addElement("value");
            value.addAttribute("key", tableValue.getKey());
            value.addAttribute("name", tableValue.getName());
            value.addAttribute("type", tableValue.getType());
            value.setText(tableValue.getValue());

        }
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("utf-8");
        XMLWriter writer = new XMLWriter(outputStream, format);
        writer.write(document);
        writer.close();
    }


    public static List<Value> loadDetail(InputStream inputStream) throws DocumentException {
        SAXReader reader = new SAXReader();
        Document document = reader.read(inputStream);
        Element root = document.getRootElement();
        List<Value> details = new ArrayList<Value>();
        for (Element value : (List<Element>) root.elements("value")) {
            String key = value.attributeValue("key");
            String name = value.attributeValue("name");
            String type = value.attributeValue("type");
            String v = value.getTextTrim();
            Value _tmpValue = new Value(key, name, v, type);
            details.add(_tmpValue);

        }
        return details;
    }


    public static Report load(InputStream inputStream) throws DocumentException {
        SAXReader reader = new SAXReader();
        Document document = reader.read(inputStream);
        Element root = document.getRootElement();
        Report report = new Report(root.attributeValue("title"));
        report.setSummary(root.elementText("summary"));
        Element tables = root.element("tables");
        if (tables != null) {
            for (Iterator it = tables.elementIterator("table"); it.hasNext(); ) {
                Element table = (Element) it.next();
                String keyTitle = table.elementTextTrim("keyTitle");
                String valueTitle = table.elementTextTrim("valueTitle");
                Table tmpTable = report.newTable(table.attributeValue("id"), table.elementText("summary"), keyTitle, valueTitle);
                tmpTable.setType(table.attributeValue("type"));
                Element values = table.element("values");
                if (null != values) {
                    for (Element value : (List<Element>) values.elements("value")) {
                        String key = value.attributeValue("key");
                        String name = value.attributeValue("name");
                        String type = value.attributeValue("type");
                        String v = value.getTextTrim();
                        Value _tmpValue = new Value(key, name, v, type);
                        String detail = value.attributeValue("detail");
                        if ("true".equals(detail)) {
                            _tmpValue.setHasDetail(true);
                        }
                        tmpTable.addCol(_tmpValue);

                    }
                }
            }
        }
        return report;
    }


    /**
     * 只加载索引文件
     *
     * @param inputStream
     * @return
     */
    public static Map<String, String> loadIndex(InputStream inputStream) {
        try {
            return load(inputStream).makeIndex();
        } catch (Exception e) {
            return null;
        }
    }
}
