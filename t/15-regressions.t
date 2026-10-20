use strict;
use lib '.';
use t::TestMLCache;

plan 'no_plan';
run_tests();

__DATA__

=== TEST 1: bulk callbacks use per-operation lock options and instance defaults
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local lock = require "resty.lock"
            local defaults = { timeout = 0.1 }
            local override = { timeout = 0.01 }
            local cache = assert(mlcache.new("bulk", "cache_shm", {
                resty_lock_opts = defaults,
            }))
            local seen = {}
            local original = lock.new
            lock.new = function(self, dict, opts)
                seen[opts] = true
                return original(self, dict, opts)
            end
            local bulk = mlcache.new_bulk()
            bulk:add("a", { resty_lock_opts = override }, function() return 1 end)
            bulk:add("b", nil, function() return 2 end)
            local res = cache:get_bulk(bulk, { concurrency = 2 })
            assert(res[1] == 1 and res[4] == 2)
            assert(seen[defaults] and seen[override])
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]

=== TEST 2: renew uses per-call lock options and preserves positive and negative values
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local lock = require "resty.lock"
            local override = { timeout = 0.01 }
            local cache = assert(mlcache.new("renew", "cache_shm", {
                ipc_shm = "ipc_shm",
                shm_miss = "cache_shm_miss",
            }))
            local original = lock.new
            lock.new = function(self, dict, opts)
                assert(opts == override)
                return original(self, dict, opts)
            end
            for _, value in ipairs({ "old", false, "new" }) do
                local expected = value or nil
                local data, err, ttl = cache:renew("key", {
                    resty_lock_opts = override,
                }, function() return expected, nil, 10 end)
                assert(data == expected and err == nil and ttl == 10, err)
                assert(cache:get("key") == expected)
                local other = expected == nil and cache.dict or cache.dict_miss
                assert(other:get("renewkey") == nil)
            end
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]

=== TEST 3: resurrection keeps raw data in L2 and serializes exactly once
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local buffer = require "string.buffer"
            local cache = assert(mlcache.new("resurrect", "cache_shm", {
                ttl = 0.01,
                resurrect_ttl = 10,
                shm_locks = "locks_shm",
                l1_serializer = function(value)
                    assert(type(value) == "string", "value serialized twice")
                    return { value = value }
                end,
            }))
            assert(cache:get("key", nil, function() return "raw" end).value == "raw")
            ngx.sleep(0.02)
            local value, err, level = cache:get("key", nil, function()
                return nil, "backend unavailable"
            end)
            assert(err == nil and level == 4, err)
            assert(value.value == "raw")
            assert(buffer.decode(cache.dict:get("resurrectkey")) == "raw")
            cache.lru:flush_all()
            value, err, level = cache:get("key")
            assert(value.value == "raw" and err == nil and level == 4)
            ngx.say("ok")
        }
    }
--- response_body
ok
--- error_log
backend unavailable
--- no_error_log
[error]

=== TEST 4: zero remaining TTL does not turn a negative L2 hit into an L3 lookup
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local cache = assert(mlcache.new("negative", "cache_shm"))
            cache:get("key", nil, function() return nil end)
            cache.lru:flush_all()
            local dict = cache.dict
            cache.dict = {
                get_stale = function(_, key) return dict:get_stale(key) end,
                ttl = function() return 0 end,
            }
            local value, err, level = cache:get("key", nil, function()
                error("unexpected callback")
            end)
            assert(value == nil and err == nil and level == 2, err)
            assert(cache.lru:get("key") == nil)
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]

=== TEST 5: peek distinguishes an expiring value from indefinite TTL
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local cache = assert(mlcache.new("peek", "cache_shm"))
            cache:get("finite", { ttl = 10 }, function() return "a" end)
            cache:get("infinite", { ttl = 0 }, function() return "b" end)
            local dict = cache.dict
            cache.dict = {
                get_stale = function(_, key) return dict:get_stale(key) end,
                ttl = function() return 0 end,
            }
            local ttl, err, value, stale, no_ttl = cache:peek("finite")
            assert(ttl > 0 and err == nil and value == "a" and not stale and not no_ttl)
            ttl, err, value, stale, no_ttl = cache:peek("infinite")
            assert(ttl == 0 and err == nil and value == "b" and not stale and no_ttl)
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]

=== TEST 6: IPC timeout clears L1 after an invalidation event is lost
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local buffer = require "string.buffer"
            local cache = assert(mlcache.new("lost", "cache_shm", {
                ipc_shm = "ipc_shm",
            }))
            assert(cache:set("key", nil, "old"))
            assert(cache:update())
            assert(cache:get("key") == "old")
            -- Another worker updates L2, but its invalidation is lost.
            assert(cache.dict:set("lostkey", buffer.encode("new"), 10))
            assert(ngx.shared.ipc_shm:incr("lua-resty-ipc:index", 1))
            local ok, err = cache:update(0.001)
            assert(ok == nil and err == "could not poll ipc events: timeout")
            assert(cache.lru:get("key") == nil)
            local value, get_err, level = cache:get("key")
            assert(value == "new" and get_err == nil and level == 2)
            assert(cache:update())
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]

=== TEST 7: invalid bulk options retain errors without a module filename
--- config
    location /t {
        content_by_lua_block {
            local mlcache = require "resty.mlcache"
            local cache = assert(mlcache.new("errors", "cache_shm"))
            local opts = setmetatable({}, {
                __index = function() error("custom options error", 0) end,
            })
            local bulk = mlcache.new_bulk()
            bulk:add("key", opts, function() return 1 end)
            local ok, err = pcall(cache.get_bulk, cache, bulk)
            assert(not ok and err:find("custom options error", 1, true), err)
            assert(not err:find("concatenate", 1, true), err)
            ngx.say("ok")
        }
    }
--- response_body
ok
--- no_error_log
[error]
