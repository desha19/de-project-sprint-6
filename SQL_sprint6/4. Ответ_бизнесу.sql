--Ответ бизнесу. Задание 1.
/*
Напишите итоговый запрос, который выведет на экран количество уникальных пользователей 
с группировкой по возрасту, которые хотя бы раз писали сообщения в десяти самых старых сообществах соцсети.
Используйте сателлит s_user_socdem.
*/

--Идентификаторы самых старых групп соцсети
select hk_group_id
from STV202406073__DWH.h_groups
order by registration_dt limit 10;

--Все сообщения, отправленные в полученных группах
select hk_message_id
from STV202406073__DWH.l_groups_dialogs
where hk_group_id in (select hk_group_id
                    from STV202406073__DWH.h_groups
                    order by registration_dt limit 10);
                   
--Все пользователи, которые когда-либо писали сообщения в самых старых группах
select hk_user_id
from STV202406073__DWH.l_user_message
where hk_message_id in (select hk_message_id
                        from STV202406073__DWH.l_groups_dialogs
                        where hk_group_id in (select hk_group_id
                                            from STV202406073__DWH.h_groups
                                            order by registration_dt limit 10))

/*
Количество уникальных пользователей с группировкой по возрасту, 
которые хотя бы раз писали сообщения в десяти самых старых сообществах соцсети
*/
select age,
	   count(1)
from STV202406073__DWH.s_user_socdem
where hk_user_id in (
select hk_user_id
from STV202406073__DWH.l_user_message
where hk_message_id in (select hk_message_id
                        from STV202406073__DWH.l_groups_dialogs
                        where hk_group_id in (select hk_group_id
                                            from STV202406073__DWH.h_groups
                                            order by registration_dt limit 10)))
group by age
order by age asc;