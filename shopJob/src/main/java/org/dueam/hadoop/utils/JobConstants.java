package org.dueam.hadoop.utils;


/**
 * job��������
 * @author zhuliu 2010-11-21
 *
 */
public class JobConstants {
	
	//�ļ����б�:forest�ļ�/�ɵ������ļ�/΢������
	public static String FOREST_FILENAME = "ForestStdCatDO.data";	//forest
	//public static String FOREST_FILENAME = "ForestStdCatDO-"+ JobHelper.getCurrentDayForForest()+".data";	//forest
	public static String AJUSTPROP_FILENAME = "sccp_config.txt";	//�ɵ�����
	public static String EDITEDDATA_FILENAME = JobConstants.TEMP_DIR + "/sccp_modified.txt";			//΢��������
	public static String CAT_PRICE_FILENAME = "job2output.data";			//job2�����:��Ŀ�ļ۸�ֲ�
	//����Ŀ¼������
	public static String CAT_PRICE_OLD = JobConstants.TEMP_DIR + "/catjob5olddata";				//�ϵ���Ŀ�۸��5������
	
	//�м��ļ����Ŀ¼
	public static String TEMP_DIR = "/group/tbauction/mengka.hyy/sccp";
	
	//�ɵ����Էָ����б�
	public static final String AJUSTPROP_SP1 = " ";
	public static final String AJUSTPROP_SP2 = "_";
	public static final String AJUSTPROP_SP3 = ":";
	public static final String AJUSTPROP_SP4 = ";";
	public static final int AJUSTPROP_LENGTH = 2;
	
	//�������д����ŵȵ�
	public static final String ORDER_SP = "\t";			//�����ָ���
	public static final int ORDER_AUCTION_ID = 8;		//��Ʒid
	public static final int ORDER_CREATE_TIME = 17;		//����ʱ��
	public static final int ORDER_STATUES = 15;			//������״̬
	public static final int ORDER_PRICE = 11;			//�ɽ��۸�
	public static final int ORDER_STATUES_SUCCESS = 2;	//���������Ѹ���
	
	//IPV���д����ŵȵ�
	public static final String IPV_SP = "\t";			//��Ʒ�ָ���
	public static final int IPV_AUCTION_ID = 1;			//��Ʒid
	public static final int IPV_AMOUNT = 8;				//IPV��
	
	//�˿���д����ŵȵ�
	public static final String REFUND_SP = "\t";		//��Ʒ�ָ���
	public static final int REFUND_AUCTION_ID = 1;		//��Ʒid
	public static final int REFUND_AMOUNT = 7;			//�˿��ܴ���
	
	//��Ʒ���һЩд������ŵȵ�
	public static final String AUCTION_SP = "\u0001";	//��Ʒ�ָ���
	public static final int AUCTION_CAT_INDEX = 9;		//��Ŀ
	public static final int AUCTION_PV_INDEX = 11;		//��ƷPV
	public static final int AUCTION_PRICE_INDEX = 5;	//�۸�
	public static final int AUCTION_TYPE = 8;			//һ�ڼ�
	public static final int STUFF_STATUS = 10;			//ȫ��
	public static final int AUCTION_STATUS = 4;			//����
	public static final int AUCTION_ID = 18;			//��Ʒ����id
	public static final int USER_TYPE = 17;				//�û����ͣ�0��C����   1��B����
	public static final int OPTIONS = 15;				//��Ʒ��options
	public static final int USER_ID = 30;				//�û�id
	
	//��Ʒ������ϵ�״̬����(CPV job1)
	public static final String AUCTION_YIKOUJIA = "b";	//һ�ڼ�
	public static final String AUCTION_NEW = "5";		//ȫ��
	public static final String AUCTION_ONLINE_0 = "0";	//����0
	public static final String AUCTION_ONLINE_1 = "1";	//����1
	public static final String AUCTION_OF_9 = "-9";	//CC
	
	//CPV�۸��������ս�����Կո���Ϊ�ָ�����
	public static final String CPV_PRICE_SP = " ";		//�ָ���
	
	//����
	public static final int TEMP_LENGTH = 2;			//�м������Ĭ�ϵĳ���-2
	public static final String BLANK_SP = "\t";			//�м������Ĭ�ϵ�k-v�ָ���-tab
	public static final String COMMON_SP1 = "_";		//�м������Ĭ�ϵ�v�ķָ���- _
	public static final String COMMON_SP2 = "-";		//�м������Ĭ�ϵ�v�ķָ���- -
	
	//������Ŀ�۸�ֲ�(��100һ�ν��зָ�)
	public static final double DEFAULT_COUNT = 10.0;	//Ĭ�Ϸ�ʮ��
	public static final double INTERVAL_PRICE = 100.0;	//ȷ����Ŀ�۸�ʱ,�������100Ԫ
	public static final double ALLOW_PERCENT = 0.95;	//ȷ����Ŀ�۸�ʱ,����95%������ok
	public static final double MAX_CIRCLE = 10000;		//���ѭ����100w
	
	//ͳ����Ŀ��Ʒ����
	public static final String CAT_AUCTION_SUM_KEY = "_AMOUNT";			//ͳ����Ŀ��Ʒ������key�ĺ�׺
}
