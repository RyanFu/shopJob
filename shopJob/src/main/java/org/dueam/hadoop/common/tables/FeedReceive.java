package org.dueam.hadoop.common.tables;

/**
 * �������ݱ�
 * User: windonly
 * Date: 11-5-6 ����11:06
 */
public class FeedReceive {
    public final static int id = 0;//    *�������У�����
    public final static int rated_uid = 1;//           *������������ID
    public final static int rated_user_nick = 2;//                 *���������ǳ�
    public final static int rater_uid = 3;//           *����������ID
    public final static int rater_user_nick = 4;//                 *�������ǳ�
    public final static int rater_type = 5;//            *�����߽��׽�ɫ 1�������������� 0�������������
    public final static int suspended = 6;//           "*����״̬ 0���� 1ɾ�� 2���� 3�ݲ����� 4ɾ����ָ�                     5��һ����ɾ����ֻɾ������ 6��һ����ɾ����ɾ���������֣�         7��һ��������"
    public final static int validscore = 7;//            "* ���۵ļƷ�״̬ 1�Ʒ� 0���Ʒ֣�ƽ��һ�����ڲ�����6���� ��1�������Ʒ� ��2δʹ��֧�������Ʒ� ��3δ���֧�������ײ��Ʒ� -4:14����ͬһ��Ʒ��ʽ��ײ��Ʒ�"
    public final static int rate = 8;//      *���۽����1���� 0���� -1����
    public final static int anony = 9;//       *�Ƿ����� 1����
    public final static int feedbackdate = 10;//              *��������ʱ��
    public final static int gmt_modified = 11;//              *����޸�ʱ��
    public final static int parenttradeid = 12;//               *������
    public final static int trade_id = 13;//          *����ID
    public final static int trade_closingdate = 14;//                   *���±���ʱ��
    public final static int auc_num_id = 15;//            *��������ID
    public final static int auction_id = 16;//            *��ǰ���۱�����ID
    public final static int auction_title = 17;//               *��������
    public final static int auction_price = 18;//               *�����۸�
    public final static int modify_from = 19;//             " /*�Ƿ�С���޸� 1��С���޸ģ�                     2����Աɾ����                     3����Ա�޸ģ�                     4���������ö�ɾ����                     5����������ɾ����                     6����һ��������                     7����һ����ֻ֮ɾ������                     8����һ����֮ɾ�˲�������                     9���ָ���һ����֮����                     10���ָ���һ����ֻ֮������                     11���ָ���һ����֮ɾ�˲�������                     12����һ����������� */"
    public final static int feedback_ip = 20;//             *�����ߵ�IP
    public final static int feedback = 21;//          *guo
    public final static int reply = 22;//       *��������
    public final static int edit_status = 23;//             *0���߿�-���۷�δ�޸ģ�1--�Ѿ��޸�
    public final static int import_from = 24;//             *�����ﵼ������� 0�Ա��� 1��һ������ 2��b2c�� 4����ֱ���ࣻ8�̳�ӳ�䣻56 opensearch
    public final static int snap_id = 25;//         *�����Ŀ���ID
    public final static int explain_delete_time = 26;//                     *���۽��͵�ɾ��ʱ��
    public final static int trade_finished_date = 27;//                     *�������ʱ��
    public final static int auction_virtual = 28;//                 *��Ʒ���ͣ�1������Ʒ��2��������Ʒ��
    public final static int validfeedback = 29;//               *�Ƿ������������ 1������
    public final static int sync_version = 30;//              * ͬ���汾�ֶ� TDDL�и���ʹ��
}

