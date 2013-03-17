package org.dueam.hadoop.common.tables;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.dueam.hadoop.common.util.MapUtils;
import org.dueam.hadoop.common.util.Utils;

import java.util.Map;

/**
 * ������
 *
 * @author suoni
 */
public class TcBizOrder {
    public static int BIZ_ORDER_ID = 0;
    public static int PAY_ORDER_ID = 1;
    public static int LOGISTICS_ORDER_ID = 2;
    public static int OUT_ORDER_ID = 3;
    public static int BUYER_NICK = 4;
    public static int SELLER_NICK = 5;
    public static int BUYER_ID = 6;
    public static int SELLER_ID = 7;
    public static int AUCTION_ID = 8;
    public static int SNAP_PATH = 9;
    public static int AUCTION_TITLE = 10;
    public static int AUCTION_PRICE = 11;
    public static int BUY_AMOUNT = 12;
    public static int BIZ_TYPE = 13;
    public static int MEMO = 14;
    public static int PAY_STATUS = 15;
    public static int LOGISTICS_STATUS = 16;
    public static int GMT_CREATE = 17;
    public static int GMT_MODIFIED = 18;
    public static int STATUS = 19;
    public static int SUB_BIZ_TYPE = 20;
    public static int FAIL_REASON = 21;
    public static int GOODS_URL = 22;
    public static int BUYER_RATE_STATUS = 23;
    public static int SELLER_RATE_STATUS = 24;
    public static int SELLER_MEMO = 25;
    public static int BUYER_MEMO = 26;
    public static int SELLER_FLAG = 27;
    public static int BUYER_FLAG = 28;
    public static int BUYER_MESSAGE_PATH = 29;
    public static int REFUND_STATUS = 30;
    public static int ATTRIBUTES = 31;
    public static int ATTRIBUTES_CC = 32;
    public static int IP = 33;
    public static int END_TIME = 34;
    public static int PAY_TIME = 35;
    public static int AUCTION_PICT_URL = 36;
    public static int IS_DETAIL = 37;
    public static int IS_MAIN = 38;
    public static int POINT_RATE = 39;
    public static int PARENT_ID = 40;
    public static int ADJUST_FEE = 41;
    public static int DISCOUNT_FEE = 42;
    public static int REFUND_FEE = 43;
    public static int CONFIRM_PAID_FEE = 44;
    public static int COD_STATUS = 45;
    public static int GMT_TIMEOUT = 46;

    /**
     * return total fee ( as cent ) : only in main order
     *
     * @param _allCols all cols of order
     * @return
     */
    public static long getTotalFee(String[] _allCols) {
        if (isMain(_allCols)) {
            String tf = Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "tf", "0");
            return NumberUtils.toLong(tf, 0);
        } else {
            long auctionPrice = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.AUCTION_PRICE]);
            long buyAmount = (long) NumberUtils.toDouble(_allCols[TcBizOrder.BUY_AMOUNT]);
            long adjustFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.ADJUST_FEE]);
            long discountFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.DISCOUNT_FEE]);
            long refundFee = (long) NumberUtils.toDouble(
                    _allCols[TcBizOrder.REFUND_FEE]);

            long amount = auctionPrice * buyAmount + adjustFee
                    - discountFee - refundFee;
            return amount;
        }


    }

    /**
     * get sub type of item ( as item,auction,group )
     *
     * @param _allCols
     * @return
     */
    public static String getSubType(String[] _allCols) {
        String subType = _allCols[TcBizOrder.SUB_BIZ_TYPE];
        if ("1".equals(subType)) {
            return "item";
        } else if ("2".equals(subType)) {
            return "auction";
        } else if ("3".equals(subType)) {
            return "group";
        }
        return "item";
    }

    public static boolean isEffective(String[] _allCols) {
        return "0".equals(_allCols[TcBizOrder.STATUS]);
    }

    public static boolean isMain(String[] _allCols) {
        return "1".equals(_allCols[TcBizOrder.IS_MAIN]);
    }

    public static boolean isDetail(String[] _allCols) {
        return "1".equals(_allCols[TcBizOrder.IS_DETAIL]);
    }

    public static boolean isPaied(String[] _allCols) {
        if ("2".equals(_allCols[TcBizOrder.PAY_STATUS])
                || "6".equals(_allCols[TcBizOrder.PAY_STATUS])) {
            return true;
        }
        return false;
    }

    private static Map<String, String> BIZ_TYPE_MAP = MapUtils.asMap(new String[]{"100", "ֱ��",
            "200", "�Ź�����һ�ڼ�",
            "300", "�Զ�����",
            "500", "��������",
            "600", "�ⲿ����(��׼��)",
            "610", "�ⲿ�������Ű�",
            "620", "�ⲿ����(shopEX��)",
            "630", "�Ա���������������",
            "650", "�ⲿ����ͳһ��",
            "700", "���β�Ʒ",
            "710", "�Ƶ�",
            "800", "����ƽ̨(�ɹ���)",
            "900", "�������⽻��"});

    public static String getBizType(String type) {
        String value = BIZ_TYPE_MAP.get(type);
        return (value == null ? "" : value) + "[" + type + "]";
    }


    public static void main(String[] args) {
        String input = "ֱ�� 100�� \n" +
                "�Ź�����һ�ڼ� 200��\n" +
                "�Զ����� 300�� \n" +
                "�������� 500�� \n" +
                "�ⲿ����(��׼��) 600�� \n" +
                "�ⲿ�������Ű� 610��\n" +
                "�ⲿ����(shopEX��) 620��\n" +
                "�Ա��������������� 630��\n" +
                "�ⲿ����ͳһ�� 650 ��\n" +
                "���β�Ʒ 700�� \n" +
                "�Ƶ� 710��\n" +
                "����ƽ̨(�ɹ���) 800�� \n" +
                "�������⽻�� 900";
        for (String line : input.split("\n")) {
            String key = StringUtils.strip(line, "�� ").split(" ")[1];
            String value = StringUtils.strip(line, "�� ").split(" ")[0];
            System.out.println("\"" + key + "\"," + "\"" + value + "\",");
        }
    }

    public static String bizType(String type) {

        return null;
    }

    /**
     * is b2c order
     *
     * @param _allCols
     * @return
     */
    public static boolean isB2C(String[] _allCols) {
        return "1".equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "isB2C", "0"));
    }

    /**
     * �Ƿ��ڴ�����״̬
     */
    public static boolean noPaid(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && ("1".equals(_allCols[PAY_STATUS]) ||"7".equals(_allCols[PAY_STATUS])  );
    }

     /**
     * �Ƿ�δȷ���ջ�
     */
    public static boolean noCom(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && "2".equals(_allCols[LOGISTICS_STATUS]) && ("2".equals(_allCols[PAY_STATUS])   );
    }

    /**
     * �Ƿ�δȷ���ջ�
     */
    public static boolean noRate(String[] _allCols){
        return "0".equals(_allCols[STATUS]) && "5".equals(_allCols[BUYER_RATE_STATUS]) && ("6".equals(_allCols[PAY_STATUS])  );
    }

    /**
     * ���������key
     */
    public static final String[] ALLOW_ATTRIBUTE_KEY = new String[]{
            "wap", //�Ƿ���wap
            "bShow", //�Ƿ������buyerShow
            "vip", //�Ƿ�vip����
            "ppay", //֧�������⸶
            "suitBuyer", //�Ƿ�Ͷ�������
            "suitSeller", //�Ƿ�Ͷ��������
            "suitBuyerType",//Ͷ���������
            "suitSellerType",//Ͷ����������
            "virtual", //�Ƿ�̓�M��Ʒ
            "bnotified", //����Ѿ���������
            "snotified", //�����Ѿ��������
            "CCViewed", //�Զ������Ŀ����Ƿ�鿴��
            "rootCat", //����Ŀ
            "shipping", //������ʽ�����������ڲ�ʹ��
            "isBonus", //�Ƿ�ʹ���ۿ�ȯ
            "isCharity", //�Ƿ����
            "dg", //����
            "auto2", //�ڶ��ַ�����ʽ
            "tf", //=payOrderDO.actualTotalFee �ܼ�
            "pf", //=logisticsDO.postFee �ʷ�
            "df", //=payOrderDO.discountFee ϵͳ�ۿ�
            "tjk", //�Խ� 2008��7��11�� ������ӣ� ʯͷ������
            "j13", //�Ƿ�֧�ּ�һ���� 2008��8��5�� ������ӣ� ʯͷ������
            "7d", //�Ƿ�֧��7���������˻��� 2008��8��5�� ������ӣ� ʯͷ������
            "qk", //�Ƿ�֧��������Ʒ���緢�� 2008��8��5�� ������ӣ� ʯͷ������
            "3c3", //�Ƿ�֧��������ҵ�30��ά�� 2008��8��5�� ������ӣ� ʯͷ������
            "ps", //�Ƿ��Ѿ��������� ps = punish seller
            "px",//��Ʒ�����ID
            "mjj",//��ͼ�����ʽ�� �Ƿ��ϲ��ⶥ��1��0���ǣ�-��ͼ��ķ�ֵ-�����ֽ𣨵�λ�֣�
            "tc",  //�ײ͵�ID
            "sku",  //sku����
            "isB2C", //�Ƿ�b2c
            "mjjm",  //���̴���� ���ͼ�
            "mjmy",   //���̴���� ��������
            "mjslw",  //���̴���� ����������
            "mjsjf",   //���̴���� �����ͻ���
            "hsp",    //����ʱ�д��������ֹͨ��uicȥ�ж��Ƿ��л
            "CCSend",  //��Ҹ����ķ���״̬
            "isAP",    //�Ƿ��Կͽ���
            "point",    //ʹ�õĻ���
            "catId",    //��Ŀid
            "postPaid", //�ʷ��Ѿ��˿�/ȷ���ջ���ʽ: postPaid:bizOrderId
            "outerIdP",   //�̼��ⲿ���룬IC��Ʒ�е��ֶ�����
            "outerIdSKU",   //�̼��ⲿ���룬IC��Ʒ�е��ֶ�����
            "arf",        //���׺��˿���
            "cf",//COD����
            "ct",//cod���ǩ��ʱ��
            "cart",//���Թ��ﳵ
            "top",//��������TOP
            "vCat", //�ö����Ƿ�������������Ŀ�µ�
            "lpRate",//��ʱ���۲����Ľ��׵��ۿ���
            "ptid",//�µ�ʱ���˷�ģ��id
            "xcard", //�Ƿ�֧�����ÿ�,�������ÿ���Ŀ
            //����
            "buyerTel",//�����ϵ�绰(��������װ����)
            "opWW",//ŷ�ɿͷ�����(��������װ����)
            "tradeType",//��������װ���������ͣ���Ϊ���۽��ף��������׺��ʺŽ���
            "pwViewed",//��������װ�����ʺ��Ƿ�쿴�����쿴���Ļ���Ų쿴ʱ��
            "coop",//����������
            "peifu",//ŷ�����⸶
            //���ν���
            "spuid", //������Ҫ��spuid
            //����
            "bbcid",//������Ʒid
            "tprice",//������Ʒ���ۼ�
            "tbid", //�Ա����id                    f
            "tbnick", //�Ա����nick
            "poid",//�ⲿ�̼ұ��루��Ʒ���룩,
            "skup",//������Ʒ��sku
            //��������
            "fbuy", //��ʾ�ǿ����Ķ���
            "secKill",//��ɱ
            "codKey",//cod�޸ļ۸�ʱ֧�������ص�key
            "from",//��������Դ��Ŀǰ��������3d�Ա���ȡֵΪ��3d
            "once_t", //�Ƿ�ʹ�õ���һ���Ե��Ż�(֮�������im buy )
            "promotion", //�Ż�id(֮�������im buy )
            "cod3Fee",//cod���ҷ���,
            "dist_price_range", //�ɹ������ۼ�����
            "realRootCat", //����Ӷ����ĸ���ĿID(added by mulao,ʵʱ�������ݷ���)
            "zpbz", //֧����Ʒ����
            "instId",//֧����������Ϣ���صĻ�������,
            "payTool", //֧����������Ϣ���ص�֧������
            "accessSubType", //֧����������Ϣ���ص�֧������
            "instPayAmount", //֧����������Ϣ���ص��ʽ���
            "decreaseStore",//�����ķ�ʽ
            "market",//��ֱ�г�����
            "serialNo",//�������к�
            "ttid", //wapʹ��,��Ǳ����, �����ʶ,��ʾ�ⲿ������վ������
            "daifu", //�����������
            "peerPayerId", //�����˵�userId��֧����֧���ɹ���Ϣ�����
            "peerPayerName", //�����˵��سƣ�֧����֧���ɹ���Ϣ�����
            "peerPayerLoginId", //�����˵ĵ�¼id,֧����֧���ɹ���Ϣ�����
            "shopname",//�������
            "newsku",//���޸ĵ�sku
            "newskustatus",//���޸ĵ�sku״̬
            "outerIdNewSKU",//���޸ĵ��̼ұ���
            "shopid",//�ڵ�ĵ���id
            "lm",//�Ƿ��޸Ĺ������ջ���ַ
            "topTradeSource", //topʹ��,��ʤ����,TOP�������׵���Դ
            "topAppKey", //topʹ��,��ʤ����,TOPӦ�õı��
            "kltb", //�����Ա� ��Ӣ����
            "postTrade",//�ʼı��
            "gt2pf",//�޻��⸶
            "gt2",//��ƽ̨���ν���
            "freezeID",//��֤��id
            "prepayCat",//�ṩ��άȨƽ̨ʹ�õ��������
            "oversold", //����
            "est", //ֱ����ҵ�����ͣ����ֻ�ֱ�䣬��Ϸֱ�䣬���ֱ�䣬Ц������
            "bankfrom", //��������ʱ�����ÿ��������
            "cst", //cod�ӷ���id
            "pxjkc",//��Ʒ������Ϊ"���¼����"
            "noGoodsPS", //�޻��չҴ�������
            "meal", //�Ż�ƽ̨��Ŀ �ײ�ID ��meal:223323 (�Ӷ����ϵı��)
            "shopdt", //�Żݽ�� ���̵��ۿۼ�
            "o_promotion", //�������Ż�id
            "o_once_t", //�������Ż� �Ƿ�ʹ�õ���һ���Ե��Ż�
            "lock", //�˿����е渶ʱ�Զ�����������ǣ�1��������0��δ��������������
            "advStat", //�˿����е渶״̬����Ҫ���ڶ���չʾ��ʾ��渶״̬����������
            "advTotal", //�˿����е渶����ҵ��ܽ����˳ɹ�ʱд��
            "pn", //punish�Ƿ񴦷������ң��շ�����added by mulao
            "cartGrp", //����ͳ�ƹ��ﳵ�Ķ����������ͨ����
            "codPayTime", //cod����֧����ȷ�ϸ���ʱ��
            "yfx", //�˷��� ���豦����
            "codAF", //cod���׵�ʵ�����ã����ʵ��֧���������ķ��ã�
            "hotelConfT", //�Ƶ����ͽ��� �����ҷ������Զ�ȷ���ջ���ʱ��  ��λΪ��
            "alipayrp", //֧�����˿����
            "refundTime", //�˿��ʱ�䣬άȨ���� �溮,added by mulao
            "rType", //άȨ���ͣ�άȨ�����溮��1,2,3,4����״̬��4��ʾС�������,0��ʾ�ر�TC����
            "itemTag", //��Ʒ��ǩ  �ܽ�����
            "anony", //�������� ,��detail������չʾ��������Ķ��� �Ÿ�����
            "buyerMobile", //�����������ʱ���µ���ϵ�ֻ��� ,Ԫ������
            "activity",//��ע�����,add by huaidao
            "jhs", //�ۻ��㶩��, ��˪����
            "alipayPoint",//֧��ʱʹ�õ�֧�������ֶ��
            "AliEA2",//ֱ���̼ҵĵڶ���֧�����˺ţ�����Ӷ��۳��� �ŷ�����

            "kd_p", //������Żݣ���ս�����Ժ����attributes�����������ʾ�����򵽺��������б���һ����ӵ�tc_biz_vertical��
            "yfxFee",//�˷��ձ������ ��Ҫ������չʾ
            "yfxBizId",//�˷��ձ��ն���id ��Ҫ������չʾ
            "addCart",//���빺�ﳵ��ʱ�䣬�ܽ�����

            "cfb", //COD ��ҳе��ķ����     ���ڸ�����       add by xuezhu
            "cfp", //COD ��λѡ���û�  ����һ�������    0�����   1������  �����Ӷ���    add by xuezhu
            "cfsp",//COD ���ҳе��İٷֱ�  0<=x<=100 ����     �����Ӷ�����     add by xuezhu
            "cfs",  //COD ����Ը��Ը��е������  0�ǲ�Ը��   1��Ը��  ������������    add by xuezhu
            "cfaf", //COD ����ѵ������ add by xuezhu
            "cfm",  //COD �����Ƿ�����޸��ʷ�add by xuezhu
            "cfbi",  //COD ��ҳе��ķ���ѳ�ʹֵadd by xuezhu

            "opfx",//opensearch���ڷ��ֱ���
            "oppd",//opensearch���ڶ����Ż�����
            "opfxje",//opensearch���ڷ��ֽ��
            "opappkey",//opensearchվ���̼�appkey
            "seller_phone",//����phone
            "closeAAddSto",//���׹رպ󣬱������ӻ� ��չ������ add by baiyan
            "displayprice", //������Ʒ�ڳɽ���¼����ʾԭ�۵ı�� ˫ʮһ �̳Ǳ���������
            "payChannelCode"//֧��ǰ�ú��û�ѡ���֧����������������
    };

    /**
     * true���ֵ
     */
    public static final String ATTRIBUTE_VALUE_TRUE = "1";

    /**
     * �Ƿ��Ǿۻ���Ķ���
     *
     * @return true:�Ǿۻ��㶩��;false:���Ǿۻ���Ķ���
     */
    public static boolean isFromTgroupon(String[] _allCols) {
        return ATTRIBUTE_VALUE_TRUE
                .equals(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "jhs", "false"))
                || StringUtils.isNotBlank(Utils.getValue(_allCols[TcBizOrder.ATTRIBUTES], "tgType", ""));
    }
}
