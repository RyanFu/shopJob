package org.dueam.hadoop.common;

/**
 * A table
 * User: windonly
 * Date: 10-12-15 ÏÂÎç2:33
 */
public interface Table {
    /**
     * get table name
     * @return
     */
    String getName();

    /**
     * get format class
     * @return
     */
    Class getInputFormat();

    /**
     * get input path
     * @return
     */
    String[] getInputPath();

    /**
     * new table with new date
     * @param date
     * @return
     */
    Table newInstance(String date);

}
