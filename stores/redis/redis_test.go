package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	common "github.com/osiloke/gostore/common"
	gostoretesting "github.com/osiloke/gostore/testing"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisStoreSuite(t *testing.T) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	// Initialize the Redis client targeting our mock server
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	ctx := context.Background()
	db := NewRedisStore(ctx, client)
	defer db.Close()

	// CRUD Operations
	t.Run("Test_Save", func(t *testing.T) { gostoretesting.Test_Save(t, db) })
	t.Run("Test_Get", func(t *testing.T) { gostoretesting.Test_Get(t, db) })
	t.Run("Test_Update", func(t *testing.T) { gostoretesting.Test_Update(t, db) })
	t.Run("Test_Replace", func(t *testing.T) { gostoretesting.Test_Replace(t, db) })
	t.Run("Test_Delete", func(t *testing.T) { gostoretesting.Test_Delete(t, db) })

	// Batch Operations
	t.Run("Test_BatchInsert", func(t *testing.T) { gostoretesting.Test_BatchInsert(t, db) })
	t.Run("Test_BatchUpdate", func(t *testing.T) { gostoretesting.Test_BatchUpdate(t, db) })

	// Pagination & Navigation
	t.Run("Test_All", func(t *testing.T) { gostoretesting.Test_All(t, db) })
	t.Run("Test_Since", func(t *testing.T) { gostoretesting.Test_Since(t, db) })
	t.Run("Test_Before", func(t *testing.T) { gostoretesting.Test_Before(t, db) })

	// Querying & Projections
	t.Run("Test_GetByField", func(t *testing.T) { gostoretesting.Test_GetByField(t, db) })
	t.Run("Test_GetByFieldsByField", func(t *testing.T) { gostoretesting.Test_GetByFieldsByField(t, db) })

	// Filtered Operations
	t.Run("Test_Query", func(t *testing.T) { gostoretesting.Test_Query(t, db) })
	t.Run("Test_FilterGetAll", func(t *testing.T) { gostoretesting.Test_FilterGetAll(t, db) })
	t.Run("Test_FilterGet", func(t *testing.T) { gostoretesting.Test_FilterGet(t, db) })
	t.Run("Test_FilterUpdate", func(t *testing.T) { gostoretesting.Test_FilterUpdate(t, db) })
	t.Run("Test_FilterReplace", func(t *testing.T) { gostoretesting.Test_FilterReplace(t, db) })
	t.Run("Test_FilterDelete", func(t *testing.T) { gostoretesting.Test_FilterDelete(t, db) })

	t.Run("Test_FilterOptimization", func(t *testing.T) {
		store := "optimized_users"

		// Insert sample records
		_, err := db.Save("user1", store, map[string]interface{}{"id": "user1", "name": "Alice", "age": 30})
		require.NoError(t, err)
		_, err = db.Save("user2", store, map[string]interface{}{"id": "user2", "name": "Bob", "age": 25})
		require.NoError(t, err)

		// 1. Direct exact id match at top level
		filter1 := map[string]interface{}{"id": "user1"}
		rows1, _, err := db.Query(filter1, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item1 map[string]interface{}
		ok, err := rows1.Next(&item1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Alice", item1["name"])

		// 2. Direct exact id match nested under "q" (BadgerStore style)
		filter2 := map[string]interface{}{
			"q": map[string]interface{}{"id": "user2"},
		}
		rows2, err := db.FilterGetAll(filter2, -1, 0, store, nil)
		require.NoError(t, err)
		var item2 map[string]interface{}
		ok, err = rows2.Next(&item2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Bob", item2["name"])

		// 3. Equality operator match {"id": {"$eq": "user1"}}
		filter3 := map[string]interface{}{
			"id": map[string]interface{}{"$eq": "user1"},
		}
		rows3, _, err := db.Query(filter3, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item3 map[string]interface{}
		ok, err = rows3.Next(&item3)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Alice", item3["name"])

		// 4. Dot-notation nested key inside body: "data.id"
		filter4 := map[string]interface{}{
			"data.id": "user2",
		}
		rows4, _, err := db.Query(filter4, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item4 map[string]interface{}
		ok, err = rows4.Next(&item4)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Bob", item4["name"])

		// 5. Query primary key but also fails secondary filter check
		filter5 := map[string]interface{}{
			"id":   "user1",
			"name": "Bob", // Doesn't match Alice
		}
		rows5, _, err := db.Query(filter5, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item5 map[string]interface{}
		ok, err = rows5.Next(&item5)
		require.NoError(t, err)
		require.False(t, ok) // No match due to secondary criteria
	})

	t.Run("Test_IndexerQuerySyntax", func(t *testing.T) {
		store := "query_syntax_users"

		// Insert sample records
		_, err := db.Save("user1", store, map[string]interface{}{"id": "user1", "name": "Alice", "age": 30})
		require.NoError(t, err)
		_, err = db.Save("user2", store, map[string]interface{}{"id": "user2", "name": "Bob", "age": 25})
		require.NoError(t, err)
		_, err = db.Save("user3", store, map[string]interface{}{"id": "user3", "name": "Charlie", "age": 35})
		require.NoError(t, err)

		// 1. Negation: name = "!Alice" (matches Bob and Charlie)
		filter1 := map[string]interface{}{"name": "!Alice"}
		rows1, _, err := db.Query(filter1, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var names []string
		for {
			var item map[string]interface{}
			ok, err := rows1.Next(&item)
			require.NoError(t, err)
			if !ok {
				break
			}
			names = append(names, item["name"].(string))
		}
		require.Len(t, names, 2)
		require.Contains(t, names, "Bob")
		require.Contains(t, names, "Charlie")

		// 2. Optional/required prefixes: "+Charlie" or "?Bob"
		filter2 := map[string]interface{}{"name": "+Charlie"}
		rows2, _, err := db.Query(filter2, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item2 map[string]interface{}
		ok, err := rows2.Next(&item2)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Charlie", item2["name"])

		// 3. Regex starts-with: "^A" (matches Alice)
		filter3 := map[string]interface{}{"name": "^A"}
		rows3, _, err := db.Query(filter3, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var item3 map[string]interface{}
		ok, err = rows3.Next(&item3)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "Alice", item3["name"])

		// 4. Greater than or equal: ">30" (matches Alice (30) and Charlie (35))
		filter4 := map[string]interface{}{"age": ">30"}
		rows4, _, err := db.Query(filter4, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var ages []float64
		for {
			var item map[string]interface{}
			ok, err := rows4.Next(&item)
			require.NoError(t, err)
			if !ok {
				break
			}
			ages = append(ages, item["age"].(float64))
		}
		require.Len(t, ages, 2)
		require.Contains(t, ages, float64(30))
		require.Contains(t, ages, float64(35))

		// 5. Less than or equal: "<30"
		filter5 := map[string]interface{}{"age": "<30"}
		rows5, _, err := db.Query(filter5, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var ages5 []float64
		for {
			var item map[string]interface{}
			ok, err := rows5.Next(&item)
			require.NoError(t, err)
			if !ok {
				break
			}
			ages5 = append(ages5, item["age"].(float64))
		}
		require.Len(t, ages5, 2)
		require.Contains(t, ages5, float64(30))
		require.Contains(t, ages5, float64(25))

		// 6. Slice query values: name = []string{"Alice", "Bob"}
		filter6 := map[string]interface{}{"name": []string{"Alice", "Bob"}}
		rows6, _, err := db.Query(filter6, nil, -1, 0, store, nil)
		require.NoError(t, err)
		var names6 []string
		for {
			var item map[string]interface{}
			ok, err := rows6.Next(&item)
			require.NoError(t, err)
			if !ok {
				break
			}
			names6 = append(names6, item["name"].(string))
		}
		require.Len(t, names6, 2)
		require.Contains(t, names6, "Alice")
		require.Contains(t, names6, "Bob")
	})

	t.Run("Test_KeyStorageConfigurations", func(t *testing.T) {
		store := "ttl_users"

		// 1. Save with string TTL (1s)
		_, err := db.Save("ttl_user1", store, map[string]interface{}{
			"id":   "ttl_user1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"ttl": "1s",
			},
		})
		require.NoError(t, err)

		// 2. Save with numeric TTL (2s)
		_, err = db.Save("ttl_user2", store, map[string]interface{}{
			"id":   "ttl_user2",
			"name": "Bob",
			"_redis": map[string]interface{}{
				"ttl": 2.0,
			},
		})
		require.NoError(t, err)

		// Verify they are initially retrievable
		var item1 map[string]interface{}
		err = db.Get("ttl_user1", store, &item1)
		require.NoError(t, err)
		require.Equal(t, "Alice", item1["name"])

		// Wait 1.5 seconds so user1 expires but user2 is still active
		mr.FastForward(1500 * time.Millisecond)

		// user1 should be gone (expired)
		var item1Expired map[string]interface{}
		err = db.Get("ttl_user1", store, &item1Expired)
		require.Error(t, err)
		require.Equal(t, common.ErrNotFound, err)

		// user2 should still exist
		var item2 map[string]interface{}
		err = db.Get("ttl_user2", store, &item2)
		require.NoError(t, err)
		require.Equal(t, "Bob", item2["name"])

		// Verify ZSET self-healing on scan: querying or getting all should prune user1
		rows, err := db.All(-1, 0, store)
		require.NoError(t, err)

		var activeItems []map[string]interface{}
		for {
			var item map[string]interface{}
			ok, err := rows.Next(&item)
			require.NoError(t, err)
			if !ok {
				break
			}
			activeItems = append(activeItems, item)
		}
		require.Len(t, activeItems, 1)
		require.Equal(t, "Bob", activeItems[0]["name"])

		// After All was called, the ZSET index card should be pruned to 1!
		count, err := db.Count(store)
		require.NoError(t, err)
		require.Equal(t, 1, count) // Pruned!

		// 3. Save with persist option to clear any TTL
		_, err = db.Save("ttl_user2", store, map[string]interface{}{
			"id":   "ttl_user2",
			"name": "Bob",
			"_redis": map[string]interface{}{
				"persist": true,
			},
		})
		require.NoError(t, err)

		// Wait 1.5 seconds again (user2 would have expired if TTL of 2s was still active)
		mr.FastForward(1500 * time.Millisecond)

		// Bob should still exist because of persist!
		var item2Persisted map[string]interface{}
		err = db.Get("ttl_user2", store, &item2Persisted)
		require.NoError(t, err)
		require.Equal(t, "Bob", item2Persisted["name"])
	})

	// _redis.commands tests
	t.Run("Test_RedisCommands_DisabledByDefault", func(t *testing.T) {
		store := "cmds_disabled"

		_, err := db.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"counter:doc1", "42"},
					},
				},
			},
		})
		require.NoError(t, err)

		// Counter key should NOT exist — commands disabled by default
		_, err = client.Get(ctx, "counter:doc1").Result()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("Test_RedisCommands_SingleCommand", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET", "INCR", "LPUSH"}),
		)
		defer db2.Close()

		store := "cmds_single"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"counter:doc1", "42"},
					},
				},
			},
		})
		require.NoError(t, err)

		// Counter key should exist with the correct value
		val, err := client2.Get(ctx, "counter:doc1").Result()
		require.NoError(t, err)
		require.Equal(t, "42", val)
	})

	t.Run("Test_RedisCommands_MultipleCommandsOrder", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET", "INCR", "LPUSH"}),
		)
		defer db2.Close()

		store := "cmds_multi"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"key:a", "1"},
					},
					map[string]interface{}{
						"cmd":  "INCR",
						"args": []interface{}{"key:a"},
					},
					map[string]interface{}{
						"cmd":  "LPUSH",
						"args": []interface{}{"list:doc1", "first"},
					},
					map[string]interface{}{
						"cmd":  "LPUSH",
						"args": []interface{}{"list:doc1", "second"},
					},
				},
			},
		})
		require.NoError(t, err)

		// key:a should be "2" (SET to "1", then INCR)
		val, err := client2.Get(ctx, "key:a").Result()
		require.NoError(t, err)
		require.Equal(t, "2", val)

		// list:doc1 should be ["second", "first"] (first LPUSH "first", then "second")
		llen, err := client2.LLen(ctx, "list:doc1").Result()
		require.NoError(t, err)
		require.Equal(t, int64(2), llen)

		items, err := client2.LRange(ctx, "list:doc1", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"second", "first"}, items)
	})

	t.Run("Test_RedisCommands_BeforeSaveVsAfterSave", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET", "GET"}),
		)
		defer db2.Close()

		store := "cmds_when"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"before:key", "before-val"},
						"when": "before_save",
					},
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"after:key", "after-val"},
						"when": "after_save",
					},
				},
			},
		})
		require.NoError(t, err)

		// Both keys should exist
		beforeVal, err := client2.Get(ctx, "before:key").Result()
		require.NoError(t, err)
		require.Equal(t, "before-val", beforeVal)

		afterVal, err := client2.Get(ctx, "after:key").Result()
		require.NoError(t, err)
		require.Equal(t, "after-val", afterVal)

		// Verify the document itself exists (saved between before_save and after_save)
		var doc map[string]interface{}
		err = db2.Get("doc1", store, &doc)
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"])
	})

	t.Run("Test_RedisCommands_AllowListRejection", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET"}), // only SET allowed
		)
		defer db2.Close()

		store := "cmds_allowlist"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"allowed:key", "ok"},
					},
					map[string]interface{}{
						"cmd":  "INCR", // NOT in allow-list
						"args": []interface{}{"counter:doc1"},
					},
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"allowed:key2", "also-ok"},
					},
				},
			},
		})
		require.NoError(t, err)

		// allowed:key should exist (SET is allowed)
		val, err := client2.Get(ctx, "allowed:key").Result()
		require.NoError(t, err)
		require.Equal(t, "ok", val)

		// allowed:key2 should also exist
		val2, err := client2.Get(ctx, "allowed:key2").Result()
		require.NoError(t, err)
		require.Equal(t, "also-ok", val2)

		// counter:doc1 should NOT exist (INCR was rejected)
		_, err = client2.Get(ctx, "counter:doc1").Result()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("Test_RedisCommands_EnvelopeStripped", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET"}),
		)
		defer db2.Close()

		store := "cmds_strip"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"ttl": "1h",
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"side:key", "val"},
					},
				},
			},
		})
		require.NoError(t, err)

		// Retrieve the document and verify _redis is NOT present
		var doc map[string]interface{}
		err = db2.Get("doc1", store, &doc)
		require.NoError(t, err)
		require.Equal(t, "Alice", doc["name"])
		require.Equal(t, "doc1", doc["id"])

		// _redis must not leak into the stored document
		_, hasRedis := doc["_redis"]
		require.False(t, hasRedis, "_redis envelope should be stripped from stored document")

		// ttl should still have been applied
		ttl, err := client2.TTL(ctx, "t$cmds_strip|doc1").Result()
		require.NoError(t, err)
		require.Greater(t, ttl, time.Duration(0))
	})

	t.Run("Test_RedisCommands_DangerousCommandBlocked", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		// Test Case 1: Without explicit allow, a dangerous command (KEYS) is skipped.
		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET"}),
		)
		defer db2.Close()

		store := "cmds_dangerous"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "KEYS", // dangerous, not in allow-list
						"args": []interface{}{"*"},
					},
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"safe:key", "val"},
					},
				},
			},
		})
		require.NoError(t, err)

		// Safe command should still execute
		val, err := client2.Get(ctx, "safe:key").Result()
		require.NoError(t, err)
		require.Equal(t, "val", val)

		// Test Case 2: Using wildcard (*), dangerous commands (FLUSHDB) are still blocked by default.
		dbWildcard := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"*"}),
		)
		defer dbWildcard.Close()

		err = client2.Set(ctx, "test:keep", "alive", 0).Err()
		require.NoError(t, err)

		_, err = dbWildcard.Save("doc2", store, map[string]interface{}{
			"id":   "doc2",
			"name": "Bob",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd": "FLUSHDB", // dangerous command
					},
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"wildcard:key", "wildcard-val"},
					},
				},
			},
		})
		require.NoError(t, err)

		// The safe key wildcard:key should be set because of the * wildcard allow list
		valWildcard, err := client2.Get(ctx, "wildcard:key").Result()
		require.NoError(t, err)
		require.Equal(t, "wildcard-val", valWildcard)

		// The test:keep key should still exist because FLUSHDB is dangerous and blocked by wildcard
		keepVal, err := client2.Get(ctx, "test:keep").Result()
		require.NoError(t, err)
		require.Equal(t, "alive", keepVal)

		// Test Case 3: Explicitly allowed dangerous command (FLUSHDB) does execute even with wildcard present.
		dbExplicit := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"*", "FLUSHDB"}),
		)
		defer dbExplicit.Close()

		_, err = dbExplicit.Save("doc3", store, map[string]interface{}{
			"id":   "doc3",
			"name": "Charlie",
			"_redis": map[string]interface{}{
				"commands": []interface{}{
					map[string]interface{}{
						"cmd": "FLUSHDB",
					},
				},
			},
		})
		require.NoError(t, err)

		// The test:keep key should be gone now because FLUSHDB executed!
		_, err = client2.Get(ctx, "test:keep").Result()
		require.Equal(t, redis.Nil, err)
	})

	t.Run("Test_RedisCommands_CommandOnly", func(t *testing.T) {
		mr2, err := miniredis.Run()
		require.NoError(t, err)
		defer mr2.Close()

		client2 := redis.NewClient(&redis.Options{Addr: mr2.Addr()})
		defer client2.Close()

		db2 := NewRedisStore(ctx, client2,
			WithCustomCommandsEnabled(),
			WithAllowedCommands([]string{"SET", "INCR"}),
		)
		defer db2.Close()

		store := "cmds_only"

		_, err = db2.Save("doc1", store, map[string]interface{}{
			"id":   "doc1",
			"name": "Alice",
			"_redis": map[string]interface{}{
				"command_only": true,
				"ttl":          "1h",
				"commands": []interface{}{
					map[string]interface{}{
						"cmd":  "SET",
						"args": []interface{}{"side:counter", "10"},
						"when": "before_save",
					},
					map[string]interface{}{
						"cmd":  "INCR",
						"args": []interface{}{"side:counter"},
						"when": "after_save",
					},
				},
			},
		})
		require.NoError(t, err)

		// Both before_save and after_save commands should have executed in order
		val, err := client2.Get(ctx, "side:counter").Result()
		require.NoError(t, err)
		require.Equal(t, "11", val)

		// The document itself should NOT have been stored
		var doc map[string]interface{}
		err = db2.Get("doc1", store, &doc)
		require.Error(t, err)
		require.Equal(t, common.ErrNotFound, err)

		// TTL should NOT have been applied (no key exists to apply it to)
		ttl, err := client2.TTL(ctx, "t$cmds_only|doc1").Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-2), ttl) // -2 = key does not exist
	})
}
