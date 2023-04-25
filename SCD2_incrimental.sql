----------------------------------------------------------------------------
SELECT * FROM information_shema."columns" c



-- Подготовка данных
drop table de12.XXXX_source;
drop table de12.XXXX_stg;
drop table de12.XXXX_stg_del;
drop table de12.XXXX_target;

create table de12.XXXX_source( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

insert into de12.XXXX_source ( id, val, update_dt ) values ( 1, 'A', now() );
insert into de12.XXXX_source ( id, val, update_dt ) values ( 2, 'NEW', now() );
insert into de12.XXXX_source ( id, val, update_dt ) values ( 3, 'C', now() );
insert into de12.XXXX_source ( id, val, update_dt ) values ( 4, 'D', now() );
update de12.XXXX_source set val = 'F', update_dt = now() where id = 1;
delete from de12.XXXX_source where id = 2;

create table de12.XXXX_stg( 
	id integer,
	val varchar(50),
	update_dt timestamp(0)
);

create table de12.XXXX_stg_del( 
	id integer
);

create table de12.XXXX_target (
	id integer,
	val varchar(50),
	start_dt timestamp(0),
	end_dt timestamp(0),
	deleted_flg char(1)
);

create table de12.XXXX_meta(
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

insert into de12.XXXX_meta( schema_name, table_name, max_update_dt )
values( 'DE12','XXXX_SOURCE', to_timestamp('1900-01-01','YYYY-MM-DD') );


select * from de12.seil_source;
select * from de12.XXXX_stg;
select * from de12.XXXX_stg_del;
select * from de12.XXXX_target;
select * from de12.XXXX_meta;

update de12.XXXX_target set end_dt = '9999-12-31' where id='2' and val = 'Y';
delete from de12.XXXX_target  where val='Z';
----------------------------------------------------------------------------
-- Инкрементальная загрузка

-- 1. Очистка стейджинговых таблиц

delete from de12.XXXX_stg;
delete from de12.XXXX_stg_del;

-- 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг

insert into de12.XXXX_stg( id, val, update_dt )
select id, val, update_dt from de12.XXXX_source
where update_dt > ( select max_update_dt from de12.XXXX_meta where schema_name='DE12' and table_name='XXXX_SOURCE' );

-- 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.

insert into de12.XXXX_stg_del( id )
select id from de12.XXXX_source;

-- 4. Загрузка в приемник "вставок" на источнике (формат SCD2).

insert into de12.XXXX_target( id, val, start_dt, end_dt, deleted_flg )
select 
	stg.id, 
	stg.val, 
	stg.update_dt, 
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.XXXX_stg stg
left join de12.XXXX_target tgt
on stg.id = tgt.id
where tgt.id is null;

-- 5. Обновление в приемнике "обновлений" на источнике (формат SCD2).

update de12.XXXX_target
set 
	end_dt = tmp.update_dt - interval '1 second'
from (
	select 
		stg.id, 
		stg.update_dt 
	from de12.XXXX_stg stg
	inner join de12.XXXX_target tgt
		on stg.id = tgt.id
		and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
	where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null )
) tmp
where XXXX_target.id = tmp.id
  and XXXX_target.end_dt = to_date('9999-12-31','YYYY-MM-DD'); 

insert into de12.XXXX_target( id, val, start_dt, end_dt, deleted_flg )
select 
	stg.id, 
	stg.val,
	stg.update_dt,
	to_date('9999-12-31','YYYY-MM-DD'),
	'N'
from de12.XXXX_stg stg
inner join de12.XXXX_target tgt
	on stg.id = tgt.id
	and tgt.end_dt = update_dt - interval '1 second'
where stg.val <> tgt.val or ( stg.val is null and tgt.val is not null ) or ( stg.val is not null and tgt.val is null );


-- 6. Удаление в приемнике удаленных в источнике записей (формат SCD2).

insert into de12.XXXX_target( id, val, start_dt, end_dt, deleted_flg )
select 
	tgt.id,
	tgt.val,
	now(),
	to_date('9999-12-31','YYYY-MM-DD'),
	'Y'
from de12.XXXX_target tgt
left join de12.XXXX_stg_del stg
	on stg.id = tgt.id
where stg.id is null
  and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
  and tgt.deleted_flg = 'N';

update de12.XXXX_target
set 
	end_dt = now() - interval '1 second'
where id in (
	select tgt.id
	from de12.XXXX_target tgt
	left join de12.XXXX_stg_del stg
		on stg.id = tgt.id
	where stg.id is null
	  and tgt.end_dt = to_date('9999-12-31','YYYY-MM-DD')
      and tgt.deleted_flg = 'N')
  and XXXX_target.end_dt = to_date('9999-12-31','YYYY-MM-DD')
  and XXXX_target.deleted_flg = 'N';


-- 7. Обновление метаданных.

update de12.XXXX_meta
set max_update_dt = coalesce( (select max( update_dt ) from de12.XXXX_stg ), max_update_dt)
where schema_name='DE12' and table_name = 'XXXX_SOURCE';

-- 8. Фиксация транзакции.

commit;