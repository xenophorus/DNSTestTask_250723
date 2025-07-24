drop table if exists products;
drop table if exists branch_product;
drop table if exists rc_product;
drop table if exists log_days;
drop table if exists stores;
drop table if exists needs;

create table branch_product
(
    product_id uuid not null,
    branch_id  uuid not null,
    remain     int  not null,
    reserve    int  not null,
    transit    int  not null
);

create table rc_product
(
    product_id uuid not null,
    rc_id      uuid not null,
    remain     int  not null,
    reserve    int  not null,
    transit    int  not null
);

create table products
(
    product_id  uuid not null,
    category_id uuid not null
);

create table log_days
(
    branch_id   uuid,
    category_id uuid,
    log_days    int
);

create table stores
(
    branch_id uuid,
    priority  smallint
);

create table needs
(
    branch_id  uuid,
    product_id uuid,
    needs      int
);

-- grant pg_read_server_files to alex;

copy products (product_id, category_id)
    from 'D:\alex\dev\DNSTestTask_250723\data\u_products.csv'
    delimiter ','
    csv header
;

copy branch_product (product_id, branch_id, remain, reserve, transit)
    from 'D:\alex\dev\DNSTestTask_250723\data\u_branch_products.csv'
    delimiter ','
    csv header
;

copy rc_product (product_id, rc_id, remain, reserve, transit)
    from 'D:\alex\dev\DNSTestTask_250723\data\u_rc_products.csv'
    delimiter ','
    csv header
;

insert into log_days(select b.branch_id,
                            p.category_id,
                            7 as logdays
                     from branch_product as b
                              join products as p on b.product_id = p.product_id
                     group by b.branch_id, p.category_id)
;

insert into stores (select distinct branch_id,
                                    ceil(random() * 3) as priority
                    from log_days)
;


-- explain
-- select distinct b.branch,
--        p.category_id,
--        7 as logdays
-- from branch_product b join products p on b.product = p.product_id
-- ;

insert into needs(branch_id, product_id, needs)
    (with recursive
         categories as (select distinct ld.category_id, ld.log_days
                        from log_days as ld),
         br_reduced as
             (select br.product_id as product_id,
                     br.branch_id  as branch_id,
                     br.reserve    as branch_reserve,
                     br.remain     as branch_remain,
                     c.log_days    as log_days
              from branch_product as br
                       join rc_product as rc on br.product_id = rc.product_id
                       join products as p on br.product_id = p.product_id
                       join categories as c on p.category_id = c.category_id
              where br.remain > 0
                 or br.reserve > 0
                 or br.transit > 0
                 or rc.remain > 0
                 or rc.reserve > 0
                 or rc.transit > 0)
     select brd.branch_id,
            brd.product_id,
            case -- min(1, x) * Logdays
                when 150 < brd.branch_remain + brd.branch_reserve
                    then ceil((150 * brd.log_days) * random())
                else ceil(((brd.branch_remain + brd.branch_reserve) * brd.log_days) * random())
                end as need
     from br_reduced as brd
     );

with recursive
    categories as (select distinct ld.category_id, ld.log_days
                   from log_days as ld),
    br_reduced as
        (select br.product_id as product_id,
                br.branch_id  as branch_id,
                p.category_id as category_id,
                br.transit    as branch_transit,
                br.reserve    as branch_reserve,
                br.remain     as branch_remain,
                rc.transit    as rc_transit,
                rc.remain     as rc_remain,
                rc.reserve    as rc_reserve,
                c.log_days    as log_days
         from branch_product as br
                  join rc_product as rc on br.product_id = rc.product_id
                  join products as p on br.product_id = p.product_id
                  join categories as c on p.category_id = c.category_id
         where br.remain > 0
            or br.reserve > 0
            or br.transit > 0
            or rc.remain > 0
            or rc.reserve > 0
            or rc.transit > 0)
select count(*)
from br_reduced as brd;
