package org.dueam.services.user;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 * �û�������Ϣ��������չ����
 */
public class BaseUserDO extends TinyUserDO implements Serializable {
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    private ExtraUserDO extraUserDO;

    private static final long serialVersionUID = 5304196725559989337L;

    /**
     * ���� PASSWORD
     */
    private String password;

    /**
     * ��ʵ���� FULLNAME
     */
    private String fullname;

    /**
     * ��ϸ��ַ ADDRESS
     */
    private String address;

    /**
     * ���ڳ��е��������� CITY
     */
    private String city;

    /**
     * ����ʡ�ݵ��������� PROV
     */
    private String province;

    /**
     * �������� COUNTRY
     */
    private String country;

    /**
     * �ʱ� ZIP
     */
    private String zip;

    /**
     * �绰���� PHONE
     */
    private String phone;

    /**
     * ����¼����ʱ�� REG_DATE ԭ�ȵķϳ��ֶΡ�����������Ϊ����ǿ��������ʾ���ڡ���ǰʱ�����45�졣
     */
    private Date regDate;

    /**
     * �����ܷ� RATE_SUM ��Ϊ���ҵ����÷�ֵ��ֻ�е��ȼ�������Ǩ��ʱ��Ÿ���
     */
    private Long rateSum;

    /**
     * ���������� RATE_NUM ��Ϊ��ҵ����÷�ֵ��ֻ�е��ȼ�������Ǩ��ʱ��Ÿ���
     */
    private Long rateNum;

    /**
     * ���� BIRTHDATE
     */
    private Date birthday;

    /**
     * �û�״̬ SUSPENDED �û�״̬��0 ����;1 δ����;2 ɾ��;3 ����;-9 CC
     */
    private Integer suspended;

    /**
     * NLETTER �����˰�ȫ���� 1 ���ݿ�NUMBER��
     */
    private Integer nletter;

    /**
     * AUC_WATCH �ϳ��ֶΣ�����������Ϊ�û���¼���˳��ֻ������������ѵĹ��ܣ���ȫ����ʹ�ã���
     */
    private String aucWatch;

    /**
     * REF_USER_ID �û�ע���Ƽ���ID
     */
    private Long refUserId;

    /**
     * ���ʽ USER_ACTIVE �û��ļ��ʽ��1 �ʼ�;2 �ֻ�;3 �ֻ����ʼ�;4 �����ʼ��ϵ�У����;7 �⼤��;8 ����ע��;
     */
    private Integer userActive;

    /**
     * USER_SESSION_TIME �û�����ʱ��
     */
    private Date userSessionTime;

    /**
     * �û��ֻ���ʱ�䣬����Ǹ������ֶΣ���������δ��<br>
     * Ϊ��Ա������ӣ���Ŀ��������<br>
     * 2009��6��9�� modified by ����
     */
    private Date userEmailTime;
    /**
     * USER_REGDATE ע��ʱ��
     */
    private Date userRegDate;

    /**
     * USER_REGIP ע��ʱip
     */
    private String userRegIP;

    /**
     * USER_LEVEL ��Ա�ȼ�
     */
    private Integer userLevel;

    /**
     * USER_STYLE �շѷ�������
     */
    private Integer userStyle;

    /**
     * USER_VIEWEMAIL ע���ʱ��ѡ���Ƿ�ѡ��ͬʱע��ΪAlipay���û���û�п����ֶΣ�����user_viewemail
     */
    private int userViewEmail;

    /**
     * USER_RANK
     * �û������ȼ�����̳�����û����ʹ�á����ڻ�����ʹ�õ�,����Ʒ�ٱ��ĵط�,�и�С��ר��,����user_rank���ж�С�����...С�����=46
     * by yekai
     */
    private Long userRank;

    /**
     * USER_WEBSITE �������������Ա��������
     */
    private String town;

    /**
     * USER_FROM �û�ע����Դ��
     */
    private String userFrom;

    /**
     * USER_AIM ���뱣������ʹ𰸣�����: \'�����||��\'
     */
    private String userAim;

    /**
     * USER_YIM ���뱣��email ��ȫ����
     */
    private String userYim;

    /**
     * USER_MSNM ���������绰
     */
    private String userMsnm;

    /**
     * USER_OCC ְҵ ��
     */
    private String occupation;

    /**
     * USER_INTERESTS ��Ȥ ��
     */
    private String interests;

    /**
     * USER_NEWPASSWD �û���������ʱ���ɵ�������,��Ӧ��DB�е�bmw_users.user_newpasswd������MD5���ܵ�
     */
    private String newPassword;

    /**
     * USER_NEWPASSWD_ACTKEY ��������ʱ���У���� ȷ���û��յ� ��������� email
     * ��Ӧ��DB�е�bmw_users.user_newpasswd_actkey
     */
    private String newPasswordAckNum;

    /**
     * USER_NEWEMAIL �û����ĵ��µ�email ��Ӧ��DB�е�bmw_users.user_newemail �޸������ŵ�δ����֤������
     */
    private String newEmail;

    /**
     * ȷ���û��յ� ע��ɹ��� email/����Ϣ ��ӦDB�е�bmw_users.user_newemail_actkey ����ʱ�����֤��
     */
    private String activationAckNum;

    /**
     * ID_CARD_NUMBER ���֤����
     */
    private String idCardNumber;

    /**
     * USER_GENDER �Ա�m��ʾ�У�f��ʾŮ���ձ�ʾ����
     */
    private String gender;

    /**
     * ��Ա״̬
     */
    private int promotedType;

    /**
     * PROMOTED_DATE_1 �Ա��ȷ�����
     */
    private Date promotedDate1;

    /**
     * USERLINK_NUM  ��ʱ�洢�û�ע��ʱ�������ǿ��  (��Ա������Ŀ��)
     */
    private Long userlinkNum;

    /**
     * MOBILE_PHONE �ֻ�����
     */
    private String mobilePhone;

    /**
     * �̼���֤��ʱ��
     */
    private Date promotedDate3;

    /**
     * ������֤���߶��θ�����֤��ʱ��
     */
    private Date promotedDate2;

    /**
     * WARNS_NUM ��õľ�������
     */
    private Integer warnsNum;

    /**
     * PROMOTED_DATE_4 �ʹ�����ӵ�ʱ��
     */
    private Date promotedDate4;

    /**
     * IN_YELLOW_URL �����ʹ�����ӵ�ԭ������ӵ�id
     */
    private String inYellowUrl;

    /**
     * SNICK ����NICK ����ע���سƵļ���У��
     */
    private String snick;

    //��֪��������,denali��UserDO�����õ�
//	private int notifyWithWhat;
    /**
     * �޸�����
     */
    private Date gmtModified;

    public BaseUserDO() {

    }

    public String getActivationAckNum() {
        return activationAckNum;
    }

    public void setActivationAckNum(String activationAckNum) {
        this.activationAckNum = activationAckNum;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getAucWatch() {
        return aucWatch;
    }

    public void setAucWatch(String aucWatch) {
        this.aucWatch = aucWatch;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public ExtraUserDO getExtraUserDO() {
        return extraUserDO;
    }

    public void setExtraUserDO(ExtraUserDO extraUserDO) {
        this.extraUserDO = extraUserDO;
    }

    public String getFullname() {
        return fullname;
    }

    public void setFullname(String fullname) {
        this.fullname = fullname;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getIdCardNumber() {
        return idCardNumber;
    }

    public void setIdCardNumber(String idCardNumber) {
        this.idCardNumber = idCardNumber;
    }

    public String getInterests() {
        return interests;
    }

    public void setInterests(String interests) {
        this.interests = interests;
    }

    public String getInYellowUrl() {
        return inYellowUrl;
    }

    public void setInYellowUrl(String inYellowUrl) {
        this.inYellowUrl = inYellowUrl;
    }

    public String getMobilePhone() {
        return mobilePhone;
    }

    public void setMobilePhone(String mobilePhone) {
        this.mobilePhone = mobilePhone;
    }

    public String getNewEmail() {
        return newEmail;
    }

    public void setNewEmail(String newEmail) {
        this.newEmail = newEmail;
    }

    public String getNewPassword() {
        return newPassword;
    }

    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    public String getNewPasswordAckNum() {
        return newPasswordAckNum;
    }

    public void setNewPasswordAckNum(String newPasswordAckNum) {
        this.newPasswordAckNum = newPasswordAckNum;
    }

    public Integer getNletter() {
        return nletter;
    }

    public void setNletter(Integer nletter) {
        this.nletter = nletter;
    }

    public String getOccupation() {
        return occupation;
    }

    public void setOccupation(String occupation) {
        this.occupation = occupation;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Date getPromotedDate1() {
        return promotedDate1;
    }

    public void setPromotedDate1(Date promotedDate1) {
        this.promotedDate1 = promotedDate1;
    }

    public Date getPromotedDate2() {
        return promotedDate2;
    }

    public void setPromotedDate2(Date promotedDate2) {
        this.promotedDate2 = promotedDate2;
    }

    public Date getPromotedDate3() {
        return promotedDate3;
    }

    public void setPromotedDate3(Date promotedDate3) {
        this.promotedDate3 = promotedDate3;
    }

    public Date getPromotedDate4() {
        return promotedDate4;
    }

    public void setPromotedDate4(Date promotedDate4) {
        this.promotedDate4 = promotedDate4;
    }

    public int getPromotedType() {
        return promotedType;
    }

    public void setPromotedType(int promotedType) {
        this.promotedType = promotedType;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getRateNum() {
        return rateNum;
    }

    public void setRateNum(Long rateNum) {
        this.rateNum = rateNum;
    }

    public Long getRateSum() {
        return rateSum;
    }

    public void setRateSum(Long rateSum) {
        this.rateSum = rateSum;
    }

    public Long getRefUserId() {
        return refUserId;
    }

    public void setRefUserId(Long refUserId) {
        this.refUserId = refUserId;
    }

    public Date getRegDate() {
        return regDate;
    }

    public void setRegDate(Date regDate) {
        this.regDate = regDate;
    }

    public String getSnick() {
        return snick;
    }

    public void setSnick(String snick) {
        this.snick = snick;
    }

    public Integer getSuspended() {
        return suspended;
    }

    public void setSuspended(Integer suspended) {
        this.suspended = suspended;
    }

    public String getTown() {
        return town;
    }

    public void setTown(String town) {
        this.town = town;
    }

    public Integer getUserActive() {
        return userActive;
    }

    public void setUserActive(Integer userActive) {
        this.userActive = userActive;
    }

    public String getUserAim() {
        return userAim;
    }

    public void setUserAim(String userAim) {
        this.userAim = userAim;
    }

    public String getUserFrom() {
        return userFrom;
    }

    public void setUserFrom(String userFrom) {
        this.userFrom = userFrom;
    }

    public Integer getUserLevel() {
        return userLevel;
    }

    public void setUserLevel(Integer userLevel) {
        this.userLevel = userLevel;
    }

    public Long getUserlinkNum() {
        return userlinkNum;
    }

    public void setUserlinkNum(Long userlinkNum) {
        this.userlinkNum = userlinkNum;
    }

    public String getUserMsnm() {
        return userMsnm;
    }

    public void setUserMsnm(String userMsnm) {
        this.userMsnm = userMsnm;
    }

    public Long getUserRank() {
        return userRank;
    }

    public void setUserRank(Long userRank) {
        this.userRank = userRank;
    }

    public Date getUserRegDate() {
        return userRegDate;
    }

    public void setUserRegDate(Date userRegDate) {
        this.userRegDate = userRegDate;
    }

    public String getUserRegIP() {
        return userRegIP;
    }

    public void setUserRegIP(String userRegIP) {
        this.userRegIP = userRegIP;
    }

    public Date getUserSessionTime() {
        return userSessionTime;
    }

    public void setUserSessionTime(Date userSessionTime) {
        this.userSessionTime = userSessionTime;
    }

    public Integer getUserStyle() {
        return userStyle;
    }

    public void setUserStyle(Integer userStyle) {
        this.userStyle = userStyle;
    }

    public int getUserViewEmail() {
        return userViewEmail;
    }

    public void setUserViewEmail(int userViewEmail) {
        this.userViewEmail = userViewEmail;
    }

    public String getUserYim() {
        return userYim;
    }

    public void setUserYim(String userYim) {
        this.userYim = userYim;
    }

    public Integer getWarnsNum() {
        return warnsNum;
    }

    public void setWarnsNum(Integer warnsNum) {
        this.warnsNum = warnsNum;
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public Date getUserEmailTime() {
        return userEmailTime;
    }

    public void setUserEmailTime(Date userEmailTime) {
        this.userEmailTime = userEmailTime;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public void setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
    }

}
