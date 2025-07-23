drop table if exists products;
drop table if exists branch_product;
drop table if exists rc_product;

create table branch_product (
    product uuid not null,
    branch uuid not null,
    remain int not null,
    reserve int not null,
    transit int not null
);

create table rc_product (
    product uuid not null,
    rc uuid not null,
    remain int not null,
    reserve int not null,
    transit int not null
);

create table products (
    product_id uuid primary key,
    category_id uuid not null
);

-- grant pg_read_server_files to alex;

copy products(product_id, category_id)
from 'D:\Doxx\dev\DNSTestTask_250723\data\u_products.csv'
delimiter ','
csv header
;

copy branch_product(product, branch, remain, reserve, transit)
    from 'D:\Doxx\dev\DNSTestTask_250723\data\u_branch_products.csv'
    delimiter ','
    csv header
;

copy rc_product(product, rc, remain, reserve, transit)
    from 'D:\Doxx\dev\DNSTestTask_250723\data\u_rc_products.csv'
    delimiter ','
    csv header
;

select * from products
limit 10;

select * from branch_product
limit 10;

select * from rc_product
limit 10;