package org.dueam.services.user;

import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Date;

/**
 * 用户基本信息，包括扩展对象
 */
public class BaseUserDO extends TinyUserDO implements Serializable {
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    private ExtraUserDO extraUserDO;

    private static final long serialVersionUID = 5304196725559989337L;

    /**
     * 密码 PASSWORD
     */
    private String password;

    /**
     * 真实名字 FULLNAME
     */
    private String fullname;

    /**
     * 详细地址 ADDRESS
     */
    private String address;

    /**
     * 所在城市的中文名称 CITY
     */
    private String city;

    /**
     * 所在省份的中文名称 PROV
     */
    private String province;

    /**
     * 国家名称 COUNTRY
     */
    private String country;

    /**
     * 邮编 ZIP
     */
    private String zip;

    /**
     * 电话号码 PHONE
     */
    private String phone;

    /**
     * 本记录更新时间 REG_DATE 原先的废除字段。现在用来作为密码强度弱的提示日期。当前时间加上45天。
     */
    private Date regDate;

    /**
     * 评价总分 RATE_SUM 作为卖家的信用分值，只有当等级发生变迁的时候才更新
     */
    private Long rateSum;

    /**
     * 评价总条数 RATE_NUM 作为买家的信用分值，只有当等级发生变迁的时候才更新
     */
    private Long rateNum;

    /**
     * 生日 BIRTHDATE
     */
    private Date birthday;

    /**
     * 用户状态 SUSPENDED 用户状态：0 正常;1 未激活;2 删除;3 冻结;-9 CC
     */
    private Integer suspended;

    /**
     * NLETTER 设置了安全邮箱 1 数据库NUMBER型
     */
    private Integer nletter;

    /**
     * AUC_WATCH 废除字段，现在用来作为用户登录、退出手机或者旺旺提醒的功能（安全部门使用）。
     */
    private String aucWatch;

    /**
     * REF_USER_ID 用户注册推荐人ID
     */
    private Long refUserId;

    /**
     * 激活方式 USER_ACTIVE 用户的激活方式：1 邮件;2 手机;3 手机、邮件;4 输入邮件上的校验码;7 免激活;8 快速注册;
     */
    private Integer userActive;

    /**
     * USER_SESSION_TIME 用户激活时间
     */
    private Date userSessionTime;

    /**
     * 用户手机绑定时间，这个是复用老字段，所以名字未变<br>
     * 为会员服务化添加，项目经理南天<br>
     * 2009年6月9日 modified by 伏威
     */
    private Date userEmailTime;
    /**
     * USER_REGDATE 注册时间
     */
    private Date userRegDate;

    /**
     * USER_REGIP 注册时ip
     */
    private String userRegIP;

    /**
     * USER_LEVEL 会员等级
     */
    private Integer userLevel;

    /**
     * USER_STYLE 收费服务类型
     */
    private Integer userStyle;

    /**
     * USER_VIEWEMAIL 注册的时候选择是否选择同时注册为Alipay，用户表没有可用字段，先用user_viewemail
     */
    private int userViewEmail;

    /**
     * USER_RANK
     * 用户社区等级，论坛改造后没有在使用。现在还是有使用的,在商品举报的地方,有个小二专用,中用user_rank来判断小二身份...小二身份=46
     * by yekai
     */
    private Long userRank;

    /**
     * USER_WEBSITE 现在用作保存会员三级地区
     */
    private String town;

    /**
     * USER_FROM 用户注册来源。
     */
    private String userFrom;

    /**
     * USER_AIM 密码保护问题和答案，形如: \'问题号||答案\'
     */
    private String userAim;

    /**
     * USER_YIM 密码保护email 安全邮箱
     */
    private String userYim;

    /**
     * USER_MSNM 保存其它电话
     */
    private String userMsnm;

    /**
     * USER_OCC 职业 ＊
     */
    private String occupation;

    /**
     * USER_INTERESTS 兴趣 ＊
     */
    private String interests;

    /**
     * USER_NEWPASSWD 用户忘记密码时生成的新密码,对应于DB中的bmw_users.user_newpasswd，是用MD5加密的
     */
    private String newPassword;

    /**
     * USER_NEWPASSWD_ACTKEY 忘记密码时候的校验码 确认用户收到 忘记密码的 email
     * 对应于DB中的bmw_users.user_newpasswd_actkey
     */
    private String newPasswordAckNum;

    /**
     * USER_NEWEMAIL 用户更改的新的email 对应于DB中的bmw_users.user_newemail 修改邮箱存放的未经验证的邮箱
     */
    private String newEmail;

    /**
     * 确认用户收到 注册成功的 email/短消息 对应DB中的bmw_users.user_newemail_actkey 激活时候的验证码
     */
    private String activationAckNum;

    /**
     * ID_CARD_NUMBER 身份证号码
     */
    private String idCardNumber;

    /**
     * USER_GENDER 性别，m表示男，f表示女，空表示保密
     */
    private String gender;

    /**
     * 会员状态
     */
    private int promotedType;

    /**
     * PROMOTED_DATE_1 淘宝先锋日期
     */
    private Date promotedDate1;

    /**
     * USERLINK_NUM  暂时存储用户注册时候密码的强度  (会员服务化项目用)
     */
    private Long userlinkNum;

    /**
     * MOBILE_PHONE 手机号码
     */
    private String mobilePhone;

    /**
     * 商家认证的时间
     */
    private Date promotedDate3;

    /**
     * 个人认证或者二次个人认证的时间
     */
    private Date promotedDate2;

    /**
     * WARNS_NUM 获得的警告数量
     */
    private Integer warnsNum;

    /**
     * PROMOTED_DATE_4 赏穿黄马褂的时间
     */
    private Date promotedDate4;

    /**
     * IN_YELLOW_URL 解释赏穿黄马褂的原因的帖子的id
     */
    private String inYellowUrl;

    /**
     * SNICK 简体NICK 用作注册呢称的简繁体校验
     */
    private String snick;

    //不知道嘛玩艺,denali的UserDO中有用到
//	private int notifyWithWhat;
    /**
     * 修改日期
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
