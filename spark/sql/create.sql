DROP TABLE IF EXISTS public.tleft;

CREATE TABLE public.tleft
(
    c_str 	varchar(5),
    c_dt 	date,
    c_ts 	timestamp,
    c_num 	integer,
    c_f 	float,
    n1 		smallint,
    n2 		integer,
    n3 		bigint,
    n4		numeric,
    n5		numeric,
    b		boolean,
    tx 		text,
    sr 		integer,
    bsr		bigint
);


DROP TABLE IF EXISTS public.tright;

CREATE TABLE public.tright
(
    c_str 	varchar(5),
    c_dt 	date,
    c_ts 	timestamp,
    c_num 	integer,
    c_f 	float,
    n1 		smallint,
    n2 		integer,
    n3 		bigint,
    n4		numeric,
    n5		numeric,
    b		boolean,
    tx 		text,
    sr 		integer,
    bsr		bigint
);
