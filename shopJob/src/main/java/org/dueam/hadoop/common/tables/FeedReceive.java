package org.dueam.hadoop.common.tables;

/**
 * 评价内容表
 * User: windonly
 * Date: 11-5-6 上午11:06
 */
public class FeedReceive {
    public final static int id = 0;//    *数字序列，主键
    public final static int rated_uid = 1;//           *被评价者数字ID
    public final static int rated_user_nick = 2;//                 *被评价者昵称
    public final static int rater_uid = 3;//           *评价者数字ID
    public final static int rater_user_nick = 4;//                 *评价者昵称
    public final static int rater_type = 5;//            *评价者交易角色 1：评价者是卖家 0：评价者是买家
    public final static int suspended = 6;//           "*评价状态 0正常 1删除 2挂起 3暂不积分 4删除后恢复                     5炒一罚二删除（只删不罚） 6炒一罚二删除（删除并罚两分）         7炒一罚二挂起"
    public final static int validscore = 7;//            "* 评价的计分状态 1计分 0不计分，平衡一个月内不超过6分用 －1匿名不计分 －2未使用支付宝不计分 －3未完成支付宝交易不计分 -4:14天内同一商品多笔交易不计分"
    public final static int rate = 8;//      *评价结果：1好评 0中评 -1差评
    public final static int anony = 9;//       *是否匿名 1匿名
    public final static int feedbackdate = 10;//              *做出评价时间
    public final static int gmt_modified = 11;//              *最近修改时间
    public final static int parenttradeid = 12;//               *主订单
    public final static int trade_id = 13;//          *交易ID
    public final static int trade_closingdate = 14;//                   *拍下宝贝时间
    public final static int auc_num_id = 15;//            *宝贝数字ID
    public final static int auction_id = 16;//            *当前评价宝贝的ID
    public final static int auction_title = 17;//               *宝贝标题
    public final static int auction_price = 18;//               *宝贝价格
    public final static int modify_from = 19;//             " /*是否小二修改 1：小二修改；                     2：会员删除；                     3：会员修改；                     4：炒作信用度删除；                     5：恶意评价删除；                     6：炒一罚二挂起；                     7：炒一罚二之只删不罚；                     8：炒一罚二之删了并罚二；                     9：恢复炒一罚二之挂起；                     10：恢复炒一罚二之只不罚；                     11：恢复炒一罚二之删了并罚二；                     12：炒一罚二清零操作 */"
    public final static int feedback_ip = 20;//             *评价者的IP
    public final static int feedback = 21;//          *guo
    public final static int reply = 22;//       *解释内容
    public final static int edit_status = 23;//             *0或者空-评价方未修改；1--已经修改
    public final static int import_from = 24;//             *从哪里导入的评价 0淘宝； 1从一拍网； 2从b2c； 4虚拟直冲类；8商城映射；56 opensearch
    public final static int snap_id = 25;//         *宝贝的快照ID
    public final static int explain_delete_time = 26;//                     *评价解释的删除时间
    public final static int trade_finished_date = 27;//                     *交易完成时间
    public final static int auction_virtual = 28;//                 *商品类型（1虚拟商品，2非虚拟商品）
    public final static int validfeedback = 29;//               *是否存在评价内容 1：存在
    public final static int sync_version = 30;//              * 同步版本字段 TDDL行复制使用
}

