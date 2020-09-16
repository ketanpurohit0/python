DELETE FROM public.tleft;
INSERT INTO public.tleft (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES (NULL, '2020-03-31', '2020-03-31 00:00:00', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1, 1);
INSERT INTO public.tleft (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', NULL, '2020-03-31 00:00:00', 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2, 2);
INSERT INTO public.tleft (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', NULL, 1, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 3, 3);
INSERT INTO public.tleft (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', '2020-03-31 00:00:00', NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 4, 4);
INSERT INTO public.tleft (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', '2020-03-31 00:00:00', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 5, 5);

DELETE FROM public.tright;
INSERT INTO public.tright (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('-', '2020-03-31', '2020-03-31 00:00:00', 1, 1, 1, 2, 4, 8, 16, false, 'Text', 1, 1);
INSERT INTO public.tright (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '1900-01-01', '2020-03-31 00:00:00', 1, 1, 1, 2, 4, 8, 16, false, 'Text', 2, 2);
INSERT INTO public.tright (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', '1900-01-01 00:00:00', 1, 1, 1, 2, 4, 8, 16, false, 'Text', 3, 3);
INSERT INTO public.tright (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', '2020-03-31 00:00:00', 0, 1, 1, 2, 4, 8, 16, false, 'Text', 4, 4);
INSERT INTO public.tright (c_str, c_dt, c_ts, c_num, c_f, n1, n2, n3, n4, n5, b, tx, sr, bsr) VALUES ('val', '2020-03-31', '2020-03-31 00:00:00', 1, 0, 1, 2, 4, 8, 16, false, 'Text', 5, 5);