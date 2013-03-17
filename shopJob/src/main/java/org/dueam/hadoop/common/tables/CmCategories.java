package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;

/**
 * User: windonly
 * Date: 11-4-21 ����6:03
 */
public class CmCategories {
    public static int cat_id = 0;//    ��Ŀ��ͼID
    public static int parent_id = 1;//����Ŀ��ͼID
    public static int cat_name = 2;//��Ŀ��ͼ��
    public static int status = 3;//��Ŀ��ͼ״̬��0��ʾ������-1��ʾɾ��,1��ʾ����
    public static int short_name = 4;//������������ȡ��������ʱ����ʶ�������Ƿ�����ȡ��join_extractΪ���룬�����ַ��������롣
    public static int cat_name_path = 5;//��Ŀ��ͼ·��
    public static int related_forum_id = 6;//�����̳ID
    public static int gmt_modified = 7;//"	����޸�ʱ��"
    public static int channel = 8;//"	Ƶ�����������飬�����������"
    public static int gmt_create = 9;//"	����ʱ��"
    public static int sort_role = 10;//����Ŀ�������
    public static int sort_order = 11;//"	����ֵ���ֵܽڵ�������ֵ������"
    public static int memo = 12;//"	��ע"
    public static int feature_cc = 13;//"	�����ֶΣ�δʹ��"
    public static int main_map = 14;//�Ƿ���������Ŀ��ͼģ�壬��Ŀ��ͼ��4��ģ�壬����һ����Ϊ������ģ��
    public static int conditions = 15;//"ɸѡ�������ʽ C1512;P10005:10023|S31|S32|S��ʾSPU��C��ʾ��Ŀ��P��ʾ��Ŀ���Ա�ǩ(������������һ������ֵ)"
    public static int auction_count = 16;//Ԥ������Ʒ����
    public static int highlight = 17;//����
    public static int features = 18;//"����Ŀ��ͼ��һЩ��������ʽ: ;*h_prepayAuction:1;charityChannel:mobile;��ʾ������Ŀ��ͼ�������⸶����Ŀ�����������ֻ��Ĵ���Ƶ�������д�*�������ǻ�̳еģ�����ǰ��Ŀ��ͼ������Ŀ�����������⸶������.�����ʽǰ��Ҫ���ֺţ�����like��ѯ"
    public static int cat_type = 19;//ģ�����
    public static int creator = 20;//��Ŀ������

    public static char SPLIT_CHAR = 0x09;

    public static boolean isEffective(String[] _allCols) {
        return "0".equals(_allCols[CmCategories.status]);
    }

    /**
     * �ж�����������Ŀ��ͼ��
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
