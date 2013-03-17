select
    c.url_domain_2 as host,
    c.list_pv,
    c.list_uv,
    c.list_muv,
    c.list_uuv,
    d.ipv as ipv,
    d.ipv_uv as ipv_uv,
    (d.ipv_uv/c.list_uv)*100 as 浏览转化率,
    d.ipv_muv as ipv_muv,
    d.ipv_uuv as ipv_uuv,
    d.buy_uv as buy_uv,
    (d.buy_uv/d.ipv_uv)*100 as 购买转化率
from
(
    select
        url_domain_2,
        count(1) as list_pv,
        count(distinct if(uid is null, mid, uid)) list_uv,
        count(distinct mid) list_muv,
        count(distinct uid) list_uuv
    from
        r_atpanel_log
    where
        pt='20120221000000'
        and url_domain_2 in ('love.taobao.com', 'list.taobao.com', 'search1.taobao.com', 'search.taobao.com', 's.taobao.com', 'search8.taobao.com')
    group by
    url_domain_2
) a left outer join
(
        select
            s.refer_domain_2 as refer_domain_2,
            count(distinct uid) ipv_uuv,
            count(distinct winner_id) buy_uv
        from(
            select
                refer_domain_2,
                uid,
                aucid
            from
                r_ods_atpanel_ipv_daily
            where
                pt= concat('20120221', '000000')
                and uid != 'NULL'
                and
                uid is not null
                and refer_domain_2 in ('love.taobao.com', 'list.taobao.com', 'search1.taobao.com', 'search.taobao.com', 's.taobao.com', 'search8.taobao.com')
            group by
                refer_domain_2,
                uid,
                aucid
        )s  left outer join(
            select
                auction_id,
                winner_id
            from
                r_alipay_details
            where
                pt = concat('20120221', '000000')
            group by
                auction_id,
                winner_id
        )n on(s.uid = n.winner_id
            and s.aucid = n.auction_id)
        where
            s.uid != 'NULL'
            and s.uid is not null
            and s.aucid != 'NULL'
            and s.aucid is not null
        group by
            s.refer_domain_2
    ) b on a.url_domain_2 = b.refer_domain_2
) c
left outer join
(
    select
        s.refer_domain_2 as refer_domain_2,
        count(distinct if(s.uid is null, s.mid, s.uid)) ipv_uv,
        count(distinct mid) ipv_muv,
        count(distinct uid) ipv_uuv,
        count(distinct winner_id) buy_uv,
        sum(s.pv) as ipv
    from(
        select
            refer_domain_2,
            mid,
            uid,
            aucid,
            count(1) pv
        from
            r_ods_atpanel_ipv_daily
        where
            pt= concat('20120221', '000000')
            and refer_domain_2 in ('love.taobao.com', 'list.taobao.com', 'search1.taobao.com', 'search.taobao.com', 's.taobao.com', 'search8.taobao.com')
        group by
            refer_domain_2,
            mid,
            uid,
            aucid
    ) d  on c.refer_domain_2 = d.refer_domain_2
