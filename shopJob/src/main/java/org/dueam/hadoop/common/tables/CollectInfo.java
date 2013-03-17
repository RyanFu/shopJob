package org.dueam.hadoop.common.tables;

/**
 * User: windonly
 * Date: 11-8-12 上午11:06
 */
public class CollectInfo {
    public static int collect_info_id = 0;//         记录ID
    public static int user_id = 1;// 收藏者id
    public static int isshared = 2;//  是否隐私收藏，0 私有1 共享
    public static int gmt_create = 3;// 创建时间
    public static int note = 4;
    public static int status = 5;//状态，0表示正常，-1表示已删除（此状态已经不用）
    public static int collect_item_id = 6;//         被收藏者id，对应COLLECT_ITEM中的主键COLLECT_ITEM_ID
    public static int gmt_modified = 7;//   最后修改时间
    public static int collect_time = 8;//   收藏时间
    public static int user_nick = 9;//     收藏者nick
    public static int tag = 10;//用户标签名称，空格隔开
    public static int item_type = 11;//   "收藏对象类型 0 店铺;1 宝贝;2 blog
    public static int category = 12;
    public static int sku_id = 13;
}
