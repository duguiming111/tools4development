# 一些小工具
> 总结一下工作中会遇到的小工具，帮你快速解决对应的问题!

## 一 mongodb数据迁移
> 将一个mongodb的collection迁移到当前mongodb或者其他的mongodb。
- mongodb_sync.py
参数:
~~~
--source_host           源mongodb的ip
--source_port           源mongodb的端口号
--source_user           源mongodb的用户名
--source_passwd         源mongodb的密码
--source_db             源mongodb的db
--source_collection     源mongodb的collection

--target_host           目标mongodb的ip
--target_port           目标mongodb的端口号
--target_user           目标mongodb的用户名
--target_passwd         目标mongodb的密码
--target_db             目标mongodb的db
--target_collection     目标mongodb的collection

--chunck                每次同步迁移的数量
~~~