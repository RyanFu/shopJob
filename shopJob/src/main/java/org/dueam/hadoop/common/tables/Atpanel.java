package org.dueam.hadoop.common.tables;

public interface Atpanel {
	int time =0 ; // ����ʱ�䣬yyyyMMddhhmmss ��
	int ip =1 ; // ������ip��
	int refer =2 ; // ���ʵ�ǰ��һϵ�в�������"&"���зָ�����ʽΪ"������=����ֵ"���磺pre=������Դurl ��
	int mid =3 ; // �����߻���id ��
	int uid =4 ; // ��Աid ��
	int sid =5 ; // session id ��
	int aid =6 ; // ���꣬ԭ���ǹ��id�ģ���δ��ʹ�ã���ͬ��ռλ�� ��
	int url =7 ; // ��ǰ���ʵ�url��
	int agent =8 ; // ���ʴ�����Ϣ�������ϵͳ��������� ��
	int adid =9 ; // ���id,ǰ2λ���������ͷ���,��10��ʼ�������� : 10 �����ƹ� 11 �ʼ����� 12 �������� ��
	int amid =10; // ALL_TAOBAO_MARKET_ID ȫ��Ӫ��id,������ad_id ��
	int cmid =11; // CHANNEL_MARKET_ID Ƶ��Ӫ��id ��
	int pmid =12; // PLACE_MARKET_ID λ��Ӫ������,���Զ�����ad_id֮�� ��
	int nmid =13; // ��cookie�л�ȡ�ģ���4���ֶ���ɣ�adid:mid:sessionid:time ��
	int nuid   =14; // ��cookie�л�ȡ�ģ���4���ֶ���ɣ�adid:uid:sessionid:time ��
	int channelid =15; // Ƶ��id ��
}
