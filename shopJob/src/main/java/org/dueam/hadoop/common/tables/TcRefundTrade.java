package org.dueam.hadoop.common.tables;

import org.dueam.hadoop.common.Area;

/**
 * �˿��
 * User: windonly
 * Date: 11-6-29 ����2:49
 */
public class TcRefundTrade {
    public final static int refund_id = 0;// NUMBER 22 N �˿��
    public final static int biz_order_id = 1;// NUMBER 22 N �Ա����׺�
    public final static int out_pay_id = 2;// VARCHAR2 32 N ֧�������׺�
    public final static int return_fee = 3;// NUMBER 22 N �˿���(��ҿɵõ����)
    public final static int total_fee = 4;// NUMBER 22 N �����ܽ��
    public final static int refund_type = 5;// NUMBER 22 N "�˿����� 1��ȫ�˸���� 2�������˸����(�����˸���ҵ�Ϊ0)�� ԭ������Ǹ�ǰ��̨��ѯ�õģ������ں���û�õ���������Ϊ@deprecated�����Կ���ȥ��"
    public final static int need_return_goods = 6;// NUMBER 22 N 0 "�Ƿ���Ҫ�˻� 0������Ҫ�˻� 1����Ҫ�˻�"
    public final static int gmt_create = 7;// DATE 7 N ��������
    public final static int gmt_modified = 8;// DATE 7 N ��������
    public final static int gmt_agreement_modified = 9;// NUMBER 22 Y �˿�Э���޸�ʱ���(��ȷ�����룬��long����ʾʱ��)
    public final static int gmt_xiaoer_inter = 10;// DATE 7 Y С������ʱ��
    public final static int gmt_timeout = 11;// DATE 7 Y "��ʱʱ��: ���timeout_typeΪSTOP_TIMEOUT��gmt_timeoutΪnull"
    public final static int timeout_type = 12;// VARCHAR2 40 Y "��ʱ���� STOP_TIMEOUT��ֹͣ��ʱ WAIT_SELLER_AGREE_TIME_OUT���ȴ�������ӦЭ�鳬ʱ BUYER_MODIFY_AGREEMENT_TIME_OUT���ȴ�����޸��˿�Э�鳬ʱ WAIT_BUYER_RETURN_GOODS_TIME_OUT���ȴ�����˻���ʱ WAIT_SELLER_CONFIRM_GOODS_TIME_OUT���ȴ�����ȷ���ջ���ʱ"
    public final static int punishment_result = 13;// VARCHAR2 200 Y С����Ͷ�ߵĴ������
    public final static int refund_status = 14;// NUMBER 22 N 0 "1���˿�Э��ȴ�����ȷ�ϡ�2���˿�Э���Ѿ���ɣ��ȴ�����˻���3��������˻����ȴ�����ȷ���ջ���4���˿�رա�5���˿�ɹ� 6�����Ҳ�ͬ��Э�飬�ȴ�����޸ġ�"
    public final static int return_goods_status = 15;// NUMBER 22 N 0 "����״̬ 1�����δ�յ��� 2��������յ��� 3��������˻�"
    public final static int cs_status = 16;// NUMBER 22 N 0 "�ͷ�����״̬ 1������ͷ����� 2����Ҫ�ͷ����� 3���ͷ��Ѿ����봦���� 4���ͷ�������� 5���ͷ����ܸ���ʧ�� 6���ͷ��������"
    public final static int buyer_id = 17;// NUMBER 22 N ���Id
    public final static int seller_id = 18;// NUMBER 22 N ����Id
    public final static int buyer_nick = 19;// VARCHAR2 32 N ����ǳ�(�����ֶ�)
    public final static int seller_nick = 20;// VARCHAR2 32 N �����ǳ�(�����ֶ�)
    public final static int seller_real_name = 21;// VARCHAR2 32 Y ������ʵ����(�˻���)
    public final static int seller_address = 22;// VARCHAR2 200 Y �����˻���ַ
    public final static int seller_post = 23;// VARCHAR2 20 Y �����ʱ�
    public final static int seller_tel = 24;// VARCHAR2 20 Y ���ҵ绰
    public final static int seller_mobile = 25;// VARCHAR2 20 Y �����ֻ�
    public final static int buyer_logistics_type = 26;// NUMBER 22 Y �˻��������� 100��ƽ�� 200�����
    public final static int buyer_logistics_name = 27;// VARCHAR2 200 Y �˻�������˾����
    public final static int buyer_logistics_mailno = 28;// VARCHAR2 40 Y �˻��˵���
    public final static int refund_reason_id = 29;// NUMBER 22 Y ����˿�ԭ��Id����TM����
    public final static int refund_reason_text = 30;// VARCHAR2 100 Y ����˿�ԭ���ı�
    public final static int refund_desc = 31;// VARCHAR2 400 Y ����˿�˵��
    public final static int gmt_buyer_return_goods = 32;// DATE 7 Y ����˻�ʱ��
    public final static int goods_url = 33;// VARCHAR2 256 Y ��ƷURL(�����ֶ�)
    public final static int auction_title = 34;// VARCHAR2 60 Y ��Ʒ����(�����ֶ�)
    public final static int pay_order_id = 35;// NUMBER 22 Y ֧������ID��Ϊ���Ժ�����ܲ�ѯ����ʼ����biz_order_id
    public final static int refund_point = 36;// NUMBER 22 Y �˵Ļ��ֵ�Ǯ
    public final static int refund_coupon = 37;// NUMBER 22 Y �˵ĺ����Ǯ
    public final static int biz_type = 38;// NUMBER 22 Y *
    public final static int attributes = 39;// VARCHAR2 4000 Y ��ŵ渶�����˳ɷֽ��
    public final static int attributes_cc = 40;// NUMBER 22 Y ����attributesʱ����
    public final static int advance_status = 41;// NUMBER 22 Y ���е渶��״̬

     public static Area refundCastTimeArea =  Area.newArea(Area.TimeCallback,0,2 * 3600, 12 * 3600 , 24 * 3600 , 2 * 24 * 3600 , 5 * 24 *3600 , 7*24 * 3600 , 14 * 24 * 3600  )  ;
}
