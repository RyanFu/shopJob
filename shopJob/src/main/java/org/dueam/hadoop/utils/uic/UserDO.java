package org.dueam.hadoop.utils.uic;

import org.apache.commons.lang.math.NumberUtils;

public class UserDO {
    private String nick;
    private String id;

    public boolean isCC() {
        return "-9".equals(suspended);
    }

    public String getSuspendedStatus() {
        if ("0".equals(suspended)) {
            return "正常";
        } else if ("1".equals(suspended)) {
            return "未激活";
        } else if ("2".equals(suspended)) {
            return "删除";
        } else if ("3".equals(suspended)) {
            return "冻结";
        } else if ("4".equals(suspended)) {
            return "用户暂停";
        } else if ("-9".equals(suspended)) {
            return "CC";
        }
        return suspended;
    }

    public String getSuspended() {
        return suspended;
    }

    public void setSuspended(String suspended) {
        this.suspended = suspended;
    }

    private String suspended;

    public long getBuyerRate() {
        return buyerRate;
    }

    public void setBuyerRate(long buyerRate) {
        this.buyerRate = buyerRate;
    }

    public void setBuyerRate(String buyerRate) {
        this.buyerRate = NumberUtils.toLong(buyerRate, 0);
    }

    private long buyerRate;

    public String getNick() {
        return nick;
    }

    public long sellerRate;

    public long getSellerRate() {
        return sellerRate;
    }

    public String getSellerRateMessage() {
        return UicUtils.getRate(sellerRate);
    }

    public void setSellerRate(long sellerRate) {
        this.sellerRate = sellerRate;
    }

    public void setSellerRate(String sellerRate) {
        this.sellerRate = NumberUtils.toLong(sellerRate, 0);
    }

    public void setNick(String nick) {
        this.nick = nick;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String toString() {
        return id + "\t" + nick;
    }
}
