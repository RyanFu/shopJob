package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;

/**
 * User: windonly
 * Date: 11-4-21 下午6:03
 */
public class CmCategories {
    public static int cat_id = 0;//    类目地图ID
    public static int parent_id = 1;//父类目地图ID
    public static int cat_name = 2;//类目地图名
    public static int status = 3;//类目地图状态，0表示正常，-1表示删除,1表示屏蔽
    public static int short_name = 4;//现在用作，抽取公用属性时，标识该属性是否参与抽取。join_extract为参与，其余字符串不参与。
    public static int cat_name_path = 5;//类目地图路径
    public static int related_forum_id = 6;//相关论坛ID
    public static int gmt_modified = 7;//"	最后修改时间"
    public static int channel = 8;//"	频道，宝贝详情，快照详情调用"
    public static int gmt_create = 9;//"	创建时间"
    public static int sort_role = 10;//子类目排序规则
    public static int sort_order = 11;//"	排序值，兄弟节点根据这个值来排序"
    public static int memo = 12;//"	备注"
    public static int feature_cc = 13;//"	保留字段，未使用"
    public static int main_map = 14;//是否主导航类目地图模板，类目地图有4套模板，其中一套作为主导航模板
    public static int conditions = 15;//"筛选条件表达式 C1512;P10005:10023|S31|S32|S表示SPU，C表示类目，P表示类目属性标签(包括属性名加一个属性值)"
    public static int auction_count = 16;//预估的商品数量
    public static int highlight = 17;//高亮
    public static int features = 18;//"该类目地图的一些特征，格式: ;*h_prepayAuction:1;charityChannel:mobile;表示，该类目地图是现行赔付的类目，又是属于手机的慈善频道。其中带*的特征是会继承的，即当前类目地图的子类目都具有现行赔付的特征.这个格式前后要带分号，方便like查询"
    public static int cat_type = 19;//模板类别
    public static int creator = 20;//类目创建者

    public static char SPLIT_CHAR = 0x09;

    public static boolean isEffective(String[] _allCols) {
        return "0".equals(_allCols[CmCategories.status]);
    }

    /**
     * 判断漱玉哪套类目地图的
     *
     * @param _allCols
     * @param catTypes
     * @return
     */
    public static boolean isCatType(String[] _allCols, String... catTypes) {
        String catType = _allCols[CmCategories.cat_type];
        for (String type : catTypes) {
            if (StringUtils.equals(type, catType)) return true;
        }
        return false;
    }

    public static String CAT_TYPE_C2C = "1";
    public static String CAT_TYPE_B2C = "2";
}
