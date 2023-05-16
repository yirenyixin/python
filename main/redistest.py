import redis   # 导入redis 模块

pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
r = redis.Redis(connection_pool=pool)


a=r.llen("redis:img")
print(a)

for i in range(a):
    print(r.lindex("redis:img",i))
# for i in range(a):
#     r.lpop("redis:img")
