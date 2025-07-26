drop materialized view if exists products_to_distribute;
drop table if exists products;
drop table if exists log_days;
drop table if exists stores;
drop table if exists needs;
drop table if exists branch_product;
drop table if exists rc_product;
drop table if exists current_branch_data;

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

create table current_branch_data
(
    branch_id     uuid not null,
    prod_sum      int,
    branch_volume int
)
;

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

create unique index on products (product_id asc);
create unique index on rc_product (product_id asc);
create index on branch_product (branch_id, product_id);

insert into log_days(select b.branch_id,
                            p.category_id,
                            7 as logdays
                     from branch_product as b
                              join products as p on b.product_id = p.product_id
                     group by b.branch_id, p.category_id)
;

insert into stores (with cte as (select distinct branch_id
                                 from branch_product)
                    select branch_id,
                           ceil(random() * 3) as priority
                    from cte)
;


-- explain
-- select distinct b.branch,
--        p.category_id,
--        7 as logdays
-- from branch_product b join products p on b.product = p.product_id
-- ;

insert into needs(branch_id, product_id, needs)
    (with br_reduced as
              (select br.product_id as product_id,
                      br.branch_id  as branch_id,
                      br.reserve    as branch_reserve,
                      br.remain     as branch_remain,
                      p.category_id as category_id
               from branch_product as br
                        join rc_product as rc on br.product_id = rc.product_id
                        join products as p on br.product_id = p.product_id
               where rc.remain > 0) -- какая разница, сколько надо товара, которого нет
     select brd.branch_id,
            brd.product_id,
            case -- min(1, x) * Logdays
                when brd.branch_remain > 150
                    then ceil((150 * l.log_days) * random())
                else ceil(((brd.branch_remain) * l.log_days) * random())
                end as need
     from br_reduced as brd
              join log_days as l on brd.category_id = l.category_id
         and l.branch_id = brd.branch_id);


/*
Есть минимум два транзита по 3000, со склада и из филиала, придется их учесть для каждого филиала.
Также придется допустить, что на каждый филиал может быть только один транзит со склада и только один
транзит с филиала.
*/

insert into current_branch_data
    (with prod_volume as
              (select sum(remain) as prod_sum,
                      branch_id
               from branch_product
               group by branch_id
               order by prod_sum),
          max_br_transit as (select max(transit) as m from branch_product),
          max_rc_transit as (select max(transit) as m from rc_product)
     select branch_id,
            prod_sum +
            (select m from max_br_transit) +
            (select m from max_rc_transit),
            case
                when prod_sum +
                     (select m from max_br_transit) +
                     (select m from max_rc_transit) < 15000 then 15000
                when prod_sum +
                     (select m from max_br_transit) +
                     (select m from max_rc_transit) < 25000 then 25000
                else 42000
                end as branch_volume
     from prod_volume)
;

with cte as (select cd.branch_id as branch_id,
                    prod_sum,
                    branch_volume,
                    priority
             from current_branch_data as cd
                      join stores as s on cd.branch_id = s.branch_id)
select *,
       round(avg(prod_sum) over (partition by branch_volume), 1) as avg_prods
from cte
;

select b.transit as from_branch,
       r.transit as from_rc,
       b.product_id,
       b.branch_id
from branch_product as b
         join rc_product as r on b.product_id = r.product_id
where b.transit > 0
   or r.transit > 0
order by from_rc desc
;

drop materialized view if exists products_to_distribute;
/*
Хотят больше, чем могут вместить - коэффициент?  min_shipment


*/
create materialized view products_to_distribute as
(
select b.product_id                               as product_id,
       b.branch_id                                as branch_id,
       b.remain                                   as br_rem,
       r.remain                                   as stock_rem,
       n.needs                                    as needs,
       s.priority                                 as priority,
       cbd.branch_volume,
       cbd.branch_volume - cbd.prod_sum           as room_in_br,
       sum(needs) over (partition by b.branch_id) as br_total_need,
       category_id
from branch_product as b
         join rc_product as r on b.product_id = r.product_id
         join stores s on b.branch_id = s.branch_id
         join current_branch_data cbd on b.branch_id = cbd.branch_id
         join needs as n on b.branch_id = n.branch_id and b.product_id = n.product_id
         join products as p on b.product_id = p.product_id
where r.remain > 0
order by product_id)
;

