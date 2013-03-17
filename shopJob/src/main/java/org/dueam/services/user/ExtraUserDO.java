package org.dueam.services.user;

import java.io.Serializable;

/**
 * 用户扩展表对象
 * 
 */
public class ExtraUserDO extends VersionDO implements Serializable {
	private static final long serialVersionUID = 6611010805431306829L;
	private String userId;   //32位字符串ID
	private int failCount; // 失败次数：个位：登录；十位：密码保护
	private int sellerGoodNum = 0; // 卖家收到的好评总条数
	private int buyerGoodNum = 0; // 买家收到的好评总条数
	private int buyerNum = 0; // 买家信用总条数
	private int buyerSum = 0; // 买家信用总分
	private int sellerNum = 0; // 卖家信用总条数
	private int sellerSum = 0; // 卖家信用总分
	private int userTag = 0;	//标记位
	
	private long userTag2 = 0;  //标记位
	private long userTag3 = 0;  //标记位
	private long userTag4 = 0;  //标记位
	private long userTag5 = 0;  //标记位
	private long userTag6 = 0;  //标记位
	private long userTag7 = 0;  //标记位
	private long userTag8 = 0;  //标记位
	
    private Integer levelType; //  crm商场用户等级	
    private Integer promotedNum; //    对应等级奖励的橱窗推荐位
    
    private Integer pmsgNitify;//是否打开站内信通知.

    private long id;   //新增的数字用户id
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}


	public Integer getPmsgNitify() {
		return pmsgNitify;
	}

	public void setPmsgNitify(Integer pmsgNitify) {
		this.pmsgNitify = pmsgNitify;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public int getFailCount() {
		return failCount;
	}

	public void setFailCount(int failCount) {
		this.failCount = failCount;
	}

	public int getSellerGoodNum() {
		return sellerGoodNum;
	}

	public void setSellerGoodNum(int sellerGoodNum) {
		this.sellerGoodNum = sellerGoodNum;
	}

	public int getBuyerGoodNum() {
		return buyerGoodNum;
	}

	public void setBuyerGoodNum(int buyerGoodNum) {
		this.buyerGoodNum = buyerGoodNum;
	}

	public int getBuyerNum() {
		return buyerNum;
	}

	public void setBuyerNum(int buyerNum) {
		this.buyerNum = buyerNum;
	}

	public int getBuyerSum() {
		return buyerSum;
	}

	public void setBuyerSum(int buyerSum) {
		this.buyerSum = buyerSum;
	}

	public int getSellerNum() {
		return sellerNum;
	}

	public void setSellerNum(int sellerNum) {
		this.sellerNum = sellerNum;
	}

	public int getSellerSum() {
		return sellerSum;
	}

	public void setSellerSum(int sellerSum) {
		this.sellerSum = sellerSum;
	}

	public int getUserTag() {
		return userTag;
	}

	public void setUserTag(int userTag) {
		this.userTag = userTag;
	}
	

	public long getUserTag2() {
		return userTag2;
	}

	public void setUserTag2(long userTag2) {
		this.userTag2 = userTag2;
	}

	public long getUserTag3() {
		return userTag3;
	}

	public void setUserTag3(long userTag3) {
		this.userTag3 = userTag3;
	}

	public long getUserTag4() {
		return userTag4;
	}

	public void setUserTag4(long userTag4) {
		this.userTag4 = userTag4;
	}

	public Integer getPromotedNum() {
		return promotedNum;
	}

	public void setPromotedNum(Integer promotedNum) {
		this.promotedNum = promotedNum;
	}

	public Integer getLevelType() {
		return levelType;
	}

	public void setLevelType(Integer levelType) {
		this.levelType = levelType;
	}

	public long getUserTag5() {
		return userTag5;
	}

	public void setUserTag5(long userTag5) {
		this.userTag5 = userTag5;
	}

	public long getUserTag6() {
		return userTag6;
	}

	public void setUserTag6(long userTag6) {
		this.userTag6 = userTag6;
	}

	public long getUserTag7() {
		return userTag7;
	}

	public void setUserTag7(long userTag7) {
		this.userTag7 = userTag7;
	}

	public long getUserTag8() {
		return userTag8;
	}

	public void setUserTag8(long userTag8) {
		this.userTag8 = userTag8;
	}

	
	
}
