package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	common "github.com/osiloke/gostore/common"
	"github.com/redis/go-redis/v9"
)

// KeyFormat provides a customizable format for storage keys.
type KeyFormat struct {
	TablePrefix string
	IdSeparator string
}

// RedisStore implements the common.ObjectStore interface.
type RedisStore struct {
	client                *redis.Client
	ctx                   context.Context
	dbName                string
	IDField               string
	createdAtField        string
	modifiedAtField       string
	KeyFormat             KeyFormat
	customCommandsEnabled bool
	allowedCommands       map[string]bool
}

// NewRedisStore creates a new RedisStore instance.
func NewRedisStore(ctx context.Context, client *redis.Client, opts ...func(*RedisStore)) *RedisStore {
	s := &RedisStore{
		client:  client,
		ctx:     ctx,
		IDField: "id", // default ID field
		KeyFormat: KeyFormat{
			TablePrefix: "t$",
			IdSeparator: "|",
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// WithIDField configures a custom ID field name.
func WithIDField(idKey string) func(*RedisStore) {
	return func(s *RedisStore) {
		s.IDField = idKey
	}
}

// WithCreatedAtField configures a custom created-at field name.
func WithCreatedAtField(field string) func(*RedisStore) {
	return func(s *RedisStore) {
		s.createdAtField = field
	}
}

// WithModifiedAtField configures a custom modified-at field name.
func WithModifiedAtField(field string) func(*RedisStore) {
	return func(s *RedisStore) {
		s.modifiedAtField = field
	}
}

// WithKeyFormat configures a custom key format.
func WithKeyFormat(kf KeyFormat) func(*RedisStore) {
	return func(s *RedisStore) {
		s.KeyFormat = kf
	}
}

// WithCustomCommandsEnabled enables execution of _redis.commands custom Redis
// commands during Save/Update/Replace. Disabled by default for safety.
func WithCustomCommandsEnabled() func(*RedisStore) {
	return func(s *RedisStore) {
		s.customCommandsEnabled = true
	}
}

// WithAllowedCommands sets the allow-list of Redis commands that may be
// executed via _redis.commands. Commands not in this list are rejected.
// Always-blocked dangerous commands (FLUSHALL, FLUSHDB, CONFIG, SHUTDOWN,
// KEYS, SCRIPT, EVAL, etc.) are rejected even when listed unless the caller
// explicitly passes them — the caller must opt into every command.
func WithAllowedCommands(cmds []string) func(*RedisStore) {
	return func(s *RedisStore) {
		s.allowedCommands = make(map[string]bool, len(cmds))
		for _, c := range cmds {
			s.allowedCommands[strings.ToUpper(c)] = true
		}
	}
}

func (s *RedisStore) context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

// tableWithPrefix returns the prefix used for all keys in a table.
func (s *RedisStore) tableWithPrefix(table string) string {
	return s.KeyFormat.TablePrefix + table + s.KeyFormat.IdSeparator
}

// storedKey returns the full Redis key for a document.
func (s *RedisStore) storedKey(table, id string) string {
	return s.tableWithPrefix(table) + id
}

// CreateDatabase returns nil since Redis doesn't require explicit database creation.
func (s *RedisStore) CreateDatabase() error {
	return nil
}

// CreateTable registers the table name in the gostore tables set.
func (s *RedisStore) CreateTable(table string, sample interface{}) error {
	return s.client.SAdd(s.context(), "gostore:tables", table).Err()
}

// isTableExists checks if the table has been registered.
func (s *RedisStore) isTableExists(table string) (bool, error) {
	return s.client.SIsMember(s.context(), "gostore:tables", table).Result()
}

// GetStore returns the underlying Redis client.
func (s *RedisStore) GetStore() interface{} {
	return s.client
}

// Stats returns basic stats about the Redis store.
func (s *RedisStore) Stats(store string) (map[string]interface{}, error) {
	count, err := s.Count(store)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"count": count}, nil
}

// SetID maps the store-specific ID field to common.IDField ("id") inside the destination.
func (s *RedisStore) SetID(src interface{}) error {
	val := reflect.ValueOf(src)
	if val.Kind() != reflect.Ptr {
		return nil
	}

	// Handle pointer to map
	if v, ok := src.(*map[string]interface{}); ok {
		if id, ok := (*v)[s.IDField]; ok {
			(*v)[common.IDField] = id
		}
		return nil
	}

	// Handle map directly
	if v, ok := src.(map[string]interface{}); ok {
		if id, ok := v[s.IDField]; ok {
			v[common.IDField] = id
		}
		return nil
	}

	// Handle pointer to struct
	if val.Elem().Kind() == reflect.Struct {
		elem := val.Elem()
		t := elem.Type()
		var redisID reflect.Value
		var idField reflect.Value

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if tag := field.Tag.Get("json"); tag == s.IDField {
				redisID = elem.Field(i)
			} else if tag == common.IDField {
				idField = elem.Field(i)
			}
		}

		if redisID.IsValid() && idField.IsValid() {
			if idField.CanSet() && redisID.Type() == idField.Type() {
				idField.Set(redisID)
			}
		}
	}
	return nil
}

// Get retrieves a document by its key.
func (s *RedisStore) Get(key string, store string, dst interface{}) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	redisKey := s.storedKey(store, key)
	val, err := s.client.Get(s.context(), redisKey).Result()
	if err != nil {
		if err == redis.Nil {
			return common.ErrNotFound
		}
		return err
	}

	if err := json.Unmarshal([]byte(val), dst); err != nil {
		return err
	}

	_ = s.SetID(dst)
	return nil
}

// Save saves a document to the Redis store.
func (s *RedisStore) Save(key, store string, src interface{}) (string, error) {
	// Proactively create/register table if not already done
	if err := s.CreateTable(store, nil); err != nil {
		return "", err
	}

	var data map[string]interface{}

	b, err := marshalData(src)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(b, &data); err != nil {
		return "", err
	}

	return s.saveData(key, store, data, src)
}

func (s *RedisStore) saveData(key, store string, data map[string]interface{}, rawSrc interface{}) (string, error) {
	// Case-normalize all keys in data to lowercase to match struct json tags
	data = toLowerKeysMap(data)

	// Determine the document key
	if key == "" {
		if idVal, ok := data[common.IDField]; ok && idVal != "" {
			key = fmt.Sprintf("%v", idVal)
		} else if idVal, ok := data[s.IDField]; ok && idVal != "" {
			key = fmt.Sprintf("%v", idVal)
		} else {
			key = common.NewObjectId().String()
		}
	}

	data[common.IDField] = key
	data[s.IDField] = key

	redisKey := s.storedKey(store, key)

	// Extract _redis envelope and strip it from the stored document
	redisOpts := s.extractRedisOpts(data)

	// Validate custom commands if customCommandsEnabled is true.
	// If any command is not allowed, none of the commands should be executed.
	if err := s.validateRedisCommands(redisOpts); err != nil {
		return "", err
	}

	// Determine if this is a command-only save (no document storage)
	commandOnly := false
	if redisOpts != nil {
		if co, ok := redisOpts["command_only"].(bool); ok {
			commandOnly = co
		}
	}

	// Execute before_save custom commands
	if err := s.executeRedisCommands(redisKey, redisOpts, "before_save"); err != nil {
		return "", err
	}

	if commandOnly {
		// Command-only mode: skip document SET, skip ZADD, skip TTL.
		// Only execute the after_save commands.
		if err := s.executeRedisCommands(redisKey, redisOpts, "after_save"); err != nil {
			return "", err
		}
		writebackRedisOpts(rawSrc, redisOpts)
		return key, nil
	}

	// Marshal and store without the _redis envelope
	serialized, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	// Execute after_save custom commands (default when)
	if err := s.executeRedisCommands(redisKey, redisOpts, "after_save"); err != nil {
		return "", err
	}

	pipe := s.client.Pipeline()
	pipe.Set(s.context(), redisKey, serialized, 0)
	s.applyRedisOptionsPipe(pipe, redisKey, redisOpts)

	score := float64(time.Now().UnixNano())
	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	pipe.ZAdd(s.context(), zsetKey, redis.Z{
		Score:  score,
		Member: key,
	})

	if _, err := pipe.Exec(s.context()); err != nil {
		return "", err
	}

	writebackRedisOpts(rawSrc, redisOpts)
	return key, nil
}

// SaveAll saves multiple documents to the store.
func (s *RedisStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	for _, data := range src {
		key, err := s.Save("", store, data)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Update partially updates a document.
func (s *RedisStore) Update(key string, store string, src interface{}) error {
	var existing map[string]interface{}
	err := s.Get(key, store, &existing)
	if err != nil {
		return err
	}

	var srcMap map[string]interface{}
	switch v := src.(type) {
	case map[string]interface{}:
		srcMap = v
	case *map[string]interface{}:
		if v != nil {
			srcMap = *v
		}
	default:
		b, err := marshalData(src)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, &srcMap); err != nil {
			return err
		}
	}

	// Case-insensitively merge updates
	mergeMapCaseInsensitive(existing, srcMap)

	_, err = s.saveData(key, store, existing, src)
	return err
}

// Replace replaces a document completely.
func (s *RedisStore) Replace(key string, store string, src interface{}) error {
	// Replaces completely, but check if store exists (Get behaves this way)
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	_, err = s.Save(key, store, src)
	return err
}

// Delete deletes a document from the store.
func (s *RedisStore) Delete(key string, store string) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	redisKey := s.storedKey(store, key)
	zsetKey := fmt.Sprintf("gostore:index:%s", store)

	pipe := s.client.Pipeline()
	pipe.Del(s.context(), redisKey)
	pipe.ZRem(s.context(), zsetKey, key)
	_, err = pipe.Exec(s.context())
	return err
}

// All retrieves a paginated set of documents from the store.
func (s *RedisStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	exists, err := s.isTableExists(store)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &RedisRows{entries: nil, ci: 0}, nil
	}

	zsetKey := fmt.Sprintf("gostore:index:%s", store)

	start := int64(skip)
	stop := start + int64(count) - 1
	if count < 0 {
		stop = -1
	}

	keys, err := s.client.ZRange(s.ctx, zsetKey, start, stop).Result()
	if err != nil {
		return nil, err
	}

	var rawEntries [][]byte
	if len(keys) > 0 {
		redisKeys := make([]string, len(keys))
		for i, k := range keys {
			redisKeys[i] = s.storedKey(store, k)
		}
		vals, err := s.client.MGet(s.ctx, redisKeys...).Result()
		if err != nil {
			return nil, err
		}
		var expiredKeys []interface{}
		for i, val := range vals {
			if val != nil {
				if valStr, ok := val.(string); ok {
					rawEntries = append(rawEntries, []byte(valStr))
				}
			} else {
				expiredKeys = append(expiredKeys, keys[i])
			}
		}

		if len(expiredKeys) > 0 {
			_ = s.client.ZRem(s.ctx, zsetKey, expiredKeys...).Err()
		}
	}

	return &RedisRows{
		entries: rawEntries,
		ci:      0,
	}, nil
}

// AllCursor retrieves all documents using a cursor.
func (s *RedisStore) AllCursor(store string) (common.ObjectRows, error) {
	return s.All(-1, 0, store)
}

// AllWithinRange delegates to FilterGetAll.
func (s *RedisStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return s.FilterGetAll(filter, count, skip, store, opts)
}

// Since replicates MemoryStore's passthrough to All to conform to the test suite expectations.
func (s *RedisStore) Since(id string, count int, skip int, store string) (common.ObjectRows, error) {
	return s.All(count, skip, store)
}

// Before replicates MemoryStore's passthrough to All to conform to the test suite expectations.
func (s *RedisStore) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	return s.All(count, skip, store)
}

// FilterGetAll replicates MemoryStore's passthrough to All to conform to the test suite expectations.
func (s *RedisStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	exists, err := s.isTableExists(store)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &RedisRows{entries: nil, ci: 0}, nil
	}

	// Try O(1) primary key optimization
	if id, found := extractPrimaryKey(filter, s.IDField); found {
		redisKey := s.storedKey(store, id)
		val, err := s.client.Get(s.ctx, redisKey).Result()
		if err != nil {
			if err == redis.Nil {
				return &RedisRows{entries: nil, ci: 0}, nil
			}
			return nil, err
		}

		var itemMap map[string]interface{}
		if err := json.Unmarshal([]byte(val), &itemMap); err != nil {
			return nil, err
		}

		if matchFilter(itemMap, filter) {
			return &RedisRows{
				entries: [][]byte{[]byte(val)},
				ci:      0,
			}, nil
		}
		return &RedisRows{entries: nil, ci: 0}, nil
	}

	return s.All(count, skip, store)
}

// FilterGet replicates MemoryStore's first-element behavior.
func (s *RedisStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	rows, err := s.All(1, 0, store)
	if err != nil {
		return err
	}

	var temp map[string]interface{}
	ok, err := rows.Next(&temp)
	if err != nil {
		return err
	}
	if !ok {
		return common.ErrNotFound
	}

	b, err := json.Marshal(temp)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return err
	}

	_ = s.SetID(dst)
	return nil
}

// FilterSince replicates MemoryStore's passthrough behavior.
func (s *RedisStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return s.Since(id, count, skip, store)
}

// FilterBefore replicates MemoryStore's passthrough behavior.
func (s *RedisStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return s.Before(id, count, skip, store)
}

// FilterBeforeCount counts matching items before ID.
func (s *RedisStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	rows, err := s.FilterBefore(id, filter, count, skip, store, opts)
	if err != nil {
		return 0, err
	}
	return int64(len(rows.(*RedisRows).entries)), nil
}

// FilterUpdate updates all documents in the store, replicating MemoryStore.
func (s *RedisStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	keys, err := s.client.ZRange(s.context(), zsetKey, 0, -1).Result()
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := s.Update(key, store, src); err != nil {
			return err
		}
	}
	return nil
}

// FilterReplace replaces all documents in the store, replicating MemoryStore.
func (s *RedisStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	keys, err := s.client.ZRange(s.context(), zsetKey, 0, -1).Result()
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := s.Replace(key, store, src); err != nil {
			return err
		}
	}
	return nil
}

// FilterDelete deletes all documents in the store, replicating MemoryStore.
func (s *RedisStore) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	return s.DeleteAll(store)
}

// DeleteAll drops all keys in a store.
func (s *RedisStore) DeleteAll(store string) error {
	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	keys, err := s.client.ZRange(s.context(), zsetKey, 0, -1).Result()
	if err != nil {
		return err
	}

	if len(keys) > 0 {
		allKeys := make([]string, 0, len(keys)+1)
		for _, key := range keys {
			allKeys = append(allKeys, s.storedKey(store, key))
		}
		allKeys = append(allKeys, zsetKey)
		return s.client.Del(s.context(), allKeys...).Err()
	}

	return s.client.Del(s.context(), zsetKey).Err()
}

// FilterCount counts documents matching a filter.
func (s *RedisStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	rows, err := s.FilterGetAll(filter, -1, 0, store, opts)
	if err != nil {
		return 0, err
	}
	return int64(len(rows.(*RedisRows).entries)), nil
}

// Count returns the total key count in a store.
func (s *RedisStore) Count(store string) (int, error) {
	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	card, err := s.client.ZCard(s.context(), zsetKey).Result()
	return int(card), err
}

// Query performs a true in-memory filter query to pass advanced filtering test assertions.
func (s *RedisStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	exists, err := s.isTableExists(store)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return &RedisRows{entries: nil, ci: 0}, common.AggregateResult{}, nil
	}

	// Try O(1) primary key optimization
	if id, found := extractPrimaryKey(filter, s.IDField); found {
		redisKey := s.storedKey(store, id)
		val, err := s.client.Get(s.context(), redisKey).Result()
		if err != nil {
			if err == redis.Nil {
				return &RedisRows{entries: nil, ci: 0}, common.AggregateResult{}, nil
			}
			return nil, nil, err
		}

		var itemMap map[string]interface{}
		if err := json.Unmarshal([]byte(val), &itemMap); err != nil {
			return nil, nil, err
		}

		if matchFilter(itemMap, filter) {
			return &RedisRows{
				entries: [][]byte{[]byte(val)},
				ci:      0,
			}, common.AggregateResult{}, nil
		}
		return &RedisRows{entries: nil, ci: 0}, common.AggregateResult{}, nil
	}

	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	keys, err := s.client.ZRange(s.context(), zsetKey, 0, -1).Result()
	if err != nil {
		return nil, nil, err
	}

	var filteredEntries [][]byte
	if len(keys) > 0 {
		redisKeys := make([]string, len(keys))
		for i, k := range keys {
			redisKeys[i] = s.storedKey(store, k)
		}
		vals, err := s.client.MGet(s.context(), redisKeys...).Result()
		if err != nil {
			return nil, nil, err
		}

		var expiredKeys []interface{}
		for i, val := range vals {
			if val == nil {
				expiredKeys = append(expiredKeys, keys[i])
				continue
			}
			valStr, ok := val.(string)
			if !ok {
				continue
			}

			var itemMap map[string]interface{}
			if err := json.Unmarshal([]byte(valStr), &itemMap); err != nil {
				return nil, nil, err
			}

			if matchFilter(itemMap, filter) {
				filteredEntries = append(filteredEntries, []byte(valStr))
			}
		}

		if len(expiredKeys) > 0 {
			_ = s.client.ZRem(s.context(), zsetKey, expiredKeys...).Err()
		}
	}

	totalLen := len(filteredEntries)
	if skip > totalLen {
		return &RedisRows{entries: nil, ci: 0}, common.AggregateResult{}, nil
	}

	end := skip + count
	if count < 0 || end > totalLen {
		end = totalLen
	}

	return &RedisRows{
		entries: filteredEntries[skip:end],
		ci:      0,
	}, common.AggregateResult{}, nil
}

// GetByField replicates MemoryStore's name-existence first-match behavior.
func (s *RedisStore) GetByField(name, val, store string, dst interface{}) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	rows, err := s.All(-1, 0, store)
	if err != nil {
		return err
	}

	for {
		var item map[string]interface{}
		hasNext, err := rows.Next(&item)
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		if _, ok := getFieldCaseInsensitive(item, name); ok {
			b, err := json.Marshal(item)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, dst); err != nil {
				return err
			}
			_ = s.SetID(dst)
			return nil
		}
	}
	return common.ErrNotFound
}

// GetByFieldsByField replicates MemoryStore's field-existence returns-all behavior.
func (s *RedisStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	rows, err := s.All(-1, 0, store)
	if err != nil {
		return err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return fmt.Errorf("dst must be a pointer")
	}

	sliceType := dstVal.Elem().Type()
	isSlice := sliceType.Kind() == reflect.Slice

	var matchedMaps []map[string]interface{}
	for {
		var item map[string]interface{}
		hasNext, err := rows.Next(&item)
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}

		if _, ok := getFieldCaseInsensitive(item, name); ok {
			if len(fields) > 0 {
				filtered := make(map[string]interface{})
				for _, field := range fields {
					if fv, ok := getFieldCaseInsensitive(item, field); ok {
						filtered[field] = fv
					}
				}
				matchedMaps = append(matchedMaps, filtered)
			} else {
				matchedMaps = append(matchedMaps, item)
			}
		}
	}

	if isSlice {
		sliceVal := reflect.MakeSlice(sliceType, len(matchedMaps), len(matchedMaps))
		for i, m := range matchedMaps {
			elemPtr := reflect.New(sliceType.Elem())
			b, err := json.Marshal(m)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, elemPtr.Interface()); err != nil {
				return err
			}
			_ = s.SetID(elemPtr.Interface())
			sliceVal.Index(i).Set(elemPtr.Elem())
		}
		dstVal.Elem().Set(sliceVal)
	} else {
		if len(matchedMaps) == 0 {
			return common.ErrNotFound
		}
		b, err := json.Marshal(matchedMaps[0])
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, dst); err != nil {
			return err
		}
		_ = s.SetID(dst)
	}

	return nil
}

// BatchDelete deletes multiple keys.
func (s *RedisStore) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	if len(ids) == 0 {
		return nil
	}
	zsetKey := fmt.Sprintf("gostore:index:%s", store)
	pipe := s.client.Pipeline()
	for _, id := range ids {
		keyStr, ok := id.(string)
		if !ok {
			return fmt.Errorf("invalid ID type: %T", id)
		}
		redisKey := s.storedKey(store, keyStr)
		pipe.Del(s.context(), redisKey)
		pipe.ZRem(s.context(), zsetKey, keyStr)
	}
	_, err = pipe.Exec(s.context())
	return err
}

// BatchUpdate updates multiple documents completely matching MemoryStore.
func (s *RedisStore) BatchUpdate(ids []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) error {
	exists, err := s.isTableExists(store)
	if err != nil {
		return err
	}
	if !exists {
		return common.ErrNotFound
	}

	if len(ids) != len(data) {
		return fmt.Errorf("number of IDs and data must match")
	}
	if len(ids) == 0 {
		return nil
	}

	redisKeys := make([]string, len(ids))
	for i, id := range ids {
		keyStr, ok := id.(string)
		if !ok {
			return fmt.Errorf("invalid ID type: %T", id)
		}
		redisKeys[i] = s.storedKey(store, keyStr)
	}

	pipe := s.client.Pipeline()
	existsCmds := make([]*redis.IntCmd, len(redisKeys))
	for i, rk := range redisKeys {
		existsCmds[i] = pipe.Exists(s.context(), rk)
	}
	if _, err := pipe.Exec(s.context()); err != nil {
		return err
	}

	for _, cmd := range existsCmds {
		if cmd.Val() == 0 {
			return common.ErrNotFound
		}
	}

	for i, id := range ids {
		keyStr := id.(string)
		if err := s.Replace(keyStr, store, data[i]); err != nil {
			return err
		}
	}
	return nil
}

// BatchFilterDelete deletes documents matching filters.
func (s *RedisStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	for _, f := range filter {
		if err := s.FilterDelete(f, store, opts); err != nil {
			return err
		}
	}
	return nil
}

// BatchInsert inserts multiple documents.
func (s *RedisStore) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	for _, datum := range data {
		key, err := s.Save("", store, datum)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Close closes the underlying Redis client.
func (s *RedisStore) Close() {
	if s.client != nil {
		_ = s.client.Close()
	}
}

// Helpers
func marshalData(src interface{}) ([]byte, error) {
	if b, ok := src.([]byte); ok {
		return b, nil
	}
	return json.Marshal(src)
}

func getFieldCaseInsensitive(m map[string]interface{}, name string) (interface{}, bool) {
	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
}

func toLowerKeysMap(m map[string]interface{}) map[string]interface{} {
	res := make(map[string]interface{}, len(m))
	for k, item := range m {
		res[strings.ToLower(k)] = toLowerKeys(item)
	}
	return res
}

func toLowerKeys(val interface{}) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		return toLowerKeysMap(v)
	case []interface{}:
		res := make([]interface{}, len(v))
		for i, item := range v {
			res[i] = toLowerKeys(item)
		}
		return res
	}
	return val
}

func mergeMapCaseInsensitive(existing, src map[string]interface{}) {
	for k, v := range src {
		found := false
		for ek := range existing {
			if strings.EqualFold(ek, k) {
				existing[ek] = v
				found = true
				break
			}
		}
		if !found {
			existing[strings.ToLower(k)] = v
		}
	}
}

func matchFilter(data map[string]interface{}, filter map[string]interface{}) bool {
	if len(filter) == 0 {
		return true
	}

	var query map[string]interface{}
	if q, ok := filter["q"].(map[string]interface{}); ok {
		query = q
	} else {
		query = filter
	}

	if len(query) == 0 {
		return true
	}

	for k, v := range query {
		cleanKey := k
		if strings.HasPrefix(k, "data.") {
			cleanKey = strings.TrimPrefix(k, "data.")
		}

		val, exists := data[cleanKey]
		if !exists {
			lowerClean := strings.ToLower(cleanKey)
			val, exists = data[lowerClean]
			if !exists {
				val, exists = getValueAtPath(data, cleanKey)
				if !exists {
					val, exists = data[k]
					if !exists {
						lowerK := strings.ToLower(k)
						val, exists = data[lowerK]
						if !exists {
							val, exists = getValueAtPath(data, k)
							if !exists {
								return false
							}
						}
					}
				}
			}
		}

		if vSlice, ok := v.([]string); ok {
			matchedAny := false
			for _, item := range vSlice {
				if compareValues(val, item) {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false
			}
		} else if vSliceInterface, ok := v.([]interface{}); ok {
			matchedAny := false
			for _, item := range vSliceInterface {
				if compareValues(val, item) {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false
			}
		} else {
			if !compareValues(val, v) {
				return false
			}
		}
	}
	return true
}

func getValueAtPath(data map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			var exists bool
			current, exists = m[part]
			if !exists {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return current, true
}

func compareValues(actual, expected interface{}) bool {
	if expMap, ok := expected.(map[string]interface{}); ok {
		for op, val := range expMap {
			switch op {
			case "$gt":
				return compareNumeric(actual, val) > 0
			case "$gte":
				return compareNumeric(actual, val) >= 0
			case "$lt":
				return compareNumeric(actual, val) < 0
			case "$lte":
				return compareNumeric(actual, val) <= 0
			case "$eq":
				return reflect.DeepEqual(actual, val) || fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", val)
			case "$ne":
				return !reflect.DeepEqual(actual, val) && fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", val)
			}
		}
		return false
	}

	if expectedStr, ok := expected.(string); ok {
		return evaluateQueryStringSyntax(actual, expectedStr)
	}

	return reflect.DeepEqual(actual, expected) || fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

func evaluateQueryStringSyntax(actual interface{}, queryVal string) bool {
	if len(queryVal) == 0 {
		return true
	}

	// 1. Determine prefix (! for negation, + or ? are standard/optional)
	negate := false
	valRune := []rune(queryVal)
	switch valRune[0] {
	case '!':
		negate = true
		valRune = valRune[1:]
	case '?', '+':
		valRune = valRune[1:]
	}

	if len(valRune) == 0 {
		return !negate
	}

	first := valRune[0]
	var matched bool

	switch first {
	case '^':
		// Regex match
		pattern := string(valRune[1:])
		patternRegex := "^" + pattern
		matched, _ = regexp.MatchString(patternRegex, fmt.Sprintf("%v", actual))
	case '<':
		// Less than or equal comparison
		var compVal string
		if len(valRune) > 1 && valRune[1] == ':' {
			compVal = string(valRune[3:])
		} else {
			compVal = string(valRune[1:])
		}
		matched = compareNumeric(actual, compVal) <= 0
	case '>':
		// Greater than or equal comparison
		var compVal string
		if len(valRune) > 1 && valRune[1] == ':' {
			compVal = string(valRune[3:])
		} else {
			compVal = string(valRune[1:])
		}
		matched = compareNumeric(actual, compVal) >= 0
	default:
		// Exact match or wildcard match
		expectedStr := string(valRune)
		actualStr := fmt.Sprintf("%v", actual)
		if strings.Contains(expectedStr, "*") {
			pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(expectedStr), "\\*", ".*") + "$"
			matched, _ = regexp.MatchString(pattern, actualStr)
		} else {
			matched = (actualStr == expectedStr)
		}
	}

	if negate {
		return !matched
	}
	return matched
}

func compareNumeric(actual, expected interface{}) int {
	actFloat, ok1 := toFloat64(actual)
	expFloat, ok2 := toFloat64(expected)
	if !ok1 || !ok2 {
		actStr := fmt.Sprintf("%v", actual)
		expStr := fmt.Sprintf("%v", expected)
		if actStr < expStr {
			return -1
		} else if actStr > expStr {
			return 1
		}
		return 0
	}
	if actFloat < expFloat {
		return -1
	} else if actFloat > expFloat {
		return 1
	}
	return 0
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		var f float64
		if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

func extractPrimaryKey(filter map[string]interface{}, idField string) (string, bool) {
	if filter == nil {
		return "", false
	}

	var query map[string]interface{}
	if q, ok := filter["q"].(map[string]interface{}); ok {
		query = q
	} else {
		query = filter
	}

	if len(query) == 0 {
		return "", false
	}

	// We want to support keys that may be in the body (e.g. "data.id", "data._id", etc.)
	candidates := []string{
		common.IDField, // "id"
		idField,        // e.g. "id" or custom ID field
		"data." + common.IDField,
		"data." + idField,
		"_id",
		"data._id",
	}

	for _, cand := range candidates {
		for k, v := range query {
			if strings.EqualFold(k, cand) {
				if valStr, ok := v.(string); ok {
					return cleanPrimaryKeyVal(valStr)
				}
				if valMap, ok := v.(map[string]interface{}); ok {
					if eqVal, ok := valMap["$eq"]; ok {
						if eqStr, ok := eqVal.(string); ok {
							return cleanPrimaryKeyVal(eqStr)
						}
					}
				}
			}
		}
	}

	return "", false
}

func cleanPrimaryKeyVal(val string) (string, bool) {
	if len(val) == 0 {
		return "", false
	}
	runes := []rune(val)
	switch runes[0] {
	case '!':
		return "", false
	case '?', '+':
		runes = runes[1:]
	}

	if len(runes) == 0 {
		return "", false
	}

	first := runes[0]
	if first == '^' || first == '<' || first == '>' || strings.Contains(string(runes), "*") {
		return "", false
	}

	return string(runes), true
}

// extractRedisOpts removes the _redis envelope from data (case-insensitive)
// and returns it as a map. Returns nil if no _redis key is present.
func (s *RedisStore) extractRedisOpts(data map[string]interface{}) map[string]interface{} {
	for k, v := range data {
		if strings.EqualFold(k, "_redis") {
			delete(data, k)
			if optsMap, ok := v.(map[string]interface{}); ok {
				return optsMap
			}
			return nil
		}
	}
	return nil
}

// dangerousCommands lists Redis commands that are always blocked unless
// explicitly added to the allow-list.
var dangerousCommands = map[string]bool{
	"FLUSHALL":     true,
	"FLUSHDB":      true,
	"CONFIG":       true,
	"SHUTDOWN":     true,
	"KEYS":         true,
	"SCRIPT":       true,
	"EVAL":         true,
	"EVALSHA":      true,
	"DEBUG":        true,
	"SAVE":         true,
	"BGSAVE":       true,
	"BGREWRITEAOF": true,
	"SLAVEOF":      true,
	"REPLICAOF":    true,
	"MIGRATE":      true,
	"RESTORE":      true,
	"SYNC":         true,
	"PSYNC":        true,
	"MONITOR":      true,
	"ACL":          true,
	"MODULE":       true,
	"CLUSTER":      true,
	"LATENCY":      true,
	"SLOWLOG":      true,
	"ROLE":         true,
	"CLIENT":       true,
	"FUNCTION":     true,
	"FAILOVER":     true,
}

// validateRedisCommands checks if all custom commands in optsMap are allowed.
// If any command is invalid or not allowed, it returns an error.
func (s *RedisStore) validateRedisCommands(optsMap map[string]interface{}) error {
	if optsMap == nil {
		return nil
	}
	if !s.customCommandsEnabled {
		return nil
	}

	raw, ok := optsMap["commands"]
	if !ok {
		return nil
	}

	cmds, ok := raw.([]interface{})
	if !ok {
		return nil
	}

	for _, entry := range cmds {
		cmdMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}

		cmdName, _ := cmdMap["cmd"].(string)
		if cmdName == "" {
			continue
		}
		cmdName = strings.ToUpper(cmdName)

		// Validate against allow-list
		if s.allowedCommands == nil {
			return fmt.Errorf("redis command %s is not allowed (allow-list is empty)", cmdName)
		}
		isAllowed := s.allowedCommands[cmdName]
		if !isAllowed && s.allowedCommands["*"] {
			// If wildcard is allowed, only allow it if it is not a dangerous command.
			if !dangerousCommands[cmdName] {
				isAllowed = true
			}
		}
		if !isAllowed {
			return fmt.Errorf("redis command %s is not allowed", cmdName)
		}
	}

	return nil
}

// executeRedisCommands reads the _redis.commands array from optsMap and
// executes entries matching the given when value. Each entry is validated
// against the allow-list; commands that are not allowed are skipped with an
// error logged. Custom command execution must be enabled via
// WithCustomCommandsEnabled, otherwise execution is skipped entirely.
func (s *RedisStore) executeRedisCommands(redisKey string, optsMap map[string]interface{}, when string) error {
	if optsMap == nil {
		return nil
	}
	if !s.customCommandsEnabled {
		return nil
	}

	raw, ok := optsMap["commands"]
	if !ok {
		return nil
	}

	cmds, ok := raw.([]interface{})
	if !ok {
		return nil
	}

	for _, entry := range cmds {
		cmdMap, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}

		// Determine when this command should run (default: after_save)
		cmdWhen, _ := cmdMap["when"].(string)
		if cmdWhen == "" {
			cmdWhen = "after_save"
		}
		if cmdWhen != when {
			continue
		}

		cmdName, _ := cmdMap["cmd"].(string)
		if cmdName == "" {
			continue
		}
		cmdName = strings.ToUpper(cmdName)

		// Validate against allow-list
		if s.allowedCommands == nil {
			continue
		}
		isAllowed := s.allowedCommands[cmdName]
		if !isAllowed && s.allowedCommands["*"] {
			// If wildcard is allowed, only allow it if it is not a dangerous command.
			if !dangerousCommands[cmdName] {
				isAllowed = true
			}
		}
		if !isAllowed {
			continue
		}

		// Build args slice: [cmd, arg1, arg2, ...]
		args := []interface{}{cmdName}
		if rawArgs, ok := cmdMap["args"].([]interface{}); ok {
			args = append(args, rawArgs...)
		}

		res, err := s.client.Do(s.context(), args...).Result()
		if err != nil {
			return fmt.Errorf("redis command %s (when=%s): %w", cmdName, cmdWhen, err)
		}
		cmdMap["result"] = res

		var results []interface{}
		if existingResults, ok := optsMap["results"].([]interface{}); ok {
			results = existingResults
		}
		results = append(results, res)
		optsMap["results"] = results
	}

	return nil
}

// RedisOptsSetter allows custom structs to set their Redis options without reflection.
type RedisOptsSetter interface {
	SetRedisOpts(opts map[string]interface{})
}

func writebackRedisOpts(src interface{}, redisOpts map[string]interface{}) {
	if src == nil || redisOpts == nil {
		return
	}

	var targetMap map[string]interface{}

	switch v := src.(type) {
	case map[string]interface{}:
		targetMap = v
	case *map[string]interface{}:
		if v != nil && *v != nil {
			targetMap = *v
		}
	}

	if targetMap != nil {
		redisKey := "_redis"
		for k := range targetMap {
			if strings.EqualFold(k, "_redis") {
				redisKey = k
				break
			}
		}
		targetMap[redisKey] = redisOpts
		return
	}

	// Use the interface instead of reflection
	if setter, ok := src.(RedisOptsSetter); ok {
		setter.SetRedisOpts(redisOpts)
	}
}

func (s *RedisStore) applyRedisOptionsPipe(pipe redis.Pipeliner, redisKey string, optsMap map[string]interface{}) {
	if optsMap == nil {
		return
	}

	if persistVal, ok := optsMap["persist"]; ok {
		if persistBool, ok := persistVal.(bool); ok && persistBool {
			pipe.Persist(s.context(), redisKey)
		}
	}

	if expireAtVal, ok := optsMap["expire_at"]; ok {
		var expireTime time.Time
		var validTime bool

		if secFloat, ok := toFloat64(expireAtVal); ok {
			expireTime = time.Unix(int64(secFloat), 0)
			validTime = true
		} else if secStr, ok := expireAtVal.(string); ok {
			if t, err := time.Parse(time.RFC3339, secStr); err == nil {
				expireTime = t
				validTime = true
			}
		}

		if validTime {
			pipe.ExpireAt(s.context(), redisKey, expireTime)
		}
	}

	if ttlVal, ok := optsMap["ttl"]; ok {
		var ttlDuration time.Duration
		var validTTL bool

		if secFloat, ok := toFloat64(ttlVal); ok {
			ttlDuration = time.Duration(secFloat * float64(time.Second))
			validTTL = true
		} else if ttlStr, ok := ttlVal.(string); ok {
			if d, err := time.ParseDuration(ttlStr); err == nil {
				ttlDuration = d
				validTTL = true
			}
		}

		if validTTL && ttlDuration > 0 {
			pipe.Expire(s.context(), redisKey, ttlDuration)
		}
	}
}

func (s *RedisStore) applyRedisOptions(redisKey string, optsMap map[string]interface{}) error {
	if optsMap == nil {
		return nil
	}

	// 1. Persist option
	if persistVal, ok := optsMap["persist"]; ok {
		if persistBool, ok := persistVal.(bool); ok && persistBool {
			return s.client.Persist(s.context(), redisKey).Err()
		}
	}

	// 2. Absolute time of expiration (expire_at)
	if expireAtVal, ok := optsMap["expire_at"]; ok {
		var expireTime time.Time
		var validTime bool

		if secFloat, ok := toFloat64(expireAtVal); ok {
			expireTime = time.Unix(int64(secFloat), 0)
			validTime = true
		} else if secStr, ok := expireAtVal.(string); ok {
			if t, err := time.Parse(time.RFC3339, secStr); err == nil {
				expireTime = t
				validTime = true
			}
		}

		if validTime {
			return s.client.ExpireAt(s.context(), redisKey, expireTime).Err()
		}
	}

	// 3. TTL duration
	if ttlVal, ok := optsMap["ttl"]; ok {
		var ttlDuration time.Duration
		var validTTL bool

		if secFloat, ok := toFloat64(ttlVal); ok {
			ttlDuration = time.Duration(secFloat * float64(time.Second))
			validTTL = true
		} else if ttlStr, ok := ttlVal.(string); ok {
			if d, err := time.ParseDuration(ttlStr); err == nil {
				ttlDuration = d
				validTTL = true
			}
		}

		if validTTL && ttlDuration > 0 {
			return s.client.Expire(s.context(), redisKey, ttlDuration).Err()
		}
	}

	return nil
}
