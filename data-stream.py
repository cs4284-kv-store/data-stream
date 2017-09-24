#! /usr/bin/env python3

import redis
r = redis.StrictRedis(host='redis')
if r.set('foo', 'bar'):
    foo = r.get('foo')
    print(foo)
