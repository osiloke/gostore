package mongodb

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/osiloke/gostore/common"
)

type MongoDBStore struct {
	client          *mongo.Client
	ctx             context.Context
	dbName          string
	createdAt       time.Time
	db              *mongo.Database
	IDField         string
	createdAtField  string
	modifiedAtField string
}

func WithIDField(idKey string) func(*MongoDBStore) {
	return func(s *MongoDBStore) {
		s.IDField = idKey
	}
}

func WithCreatedAtField(field string) func(*MongoDBStore) {
	return func(s *MongoDBStore) {
		s.createdAtField = field
	}
}

func WithModifiedAtField(field string) func(*MongoDBStore) {
	return func(s *MongoDBStore) {
		s.modifiedAtField = field
	}
}

func (s *MongoDBStore) ensureCollection(store string) error {
	// collection := s.db.Collection(store)
	// _, err := collection.Indexes().CreateOne(s.ctx, mongo.IndexModel{})
	// if err != nil {
	// 	return fmt.Errorf("failed to create collection: %w", err)
	// }
	return nil
}

func (s *MongoDBStore) Save(key string, store string, src interface{}) (string, error) {
	err := s.ensureCollection(store)
	if err != nil {
		return "", err
	}
	if data, ok := src.(map[string]interface{}); ok {
		if id, ok := data[common.IDField]; ok {
			key = id.(string)
			data[s.IDField] = id
		}
		collection := s.db.Collection(store)
		_, err = collection.InsertOne(s.ctx, data)
	}

	return key, err
}

func (s *MongoDBStore) SetID(src interface{}) error {
	val := reflect.ValueOf(src)

	if v, ok := src.(*map[string]interface{}); ok {
		if id, ok := (*v)[s.IDField]; ok {
			(*v)[common.IDField] = id
		}
		return nil
	}
	if v, ok := src.(map[string]interface{}); ok {
		if id, ok := v[s.IDField]; ok {
			v[common.IDField] = id
		}
		return nil
	}

	if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct {
		val = val.Elem()
		t := val.Type()
		var mongoID reflect.Value
		var idField reflect.Value

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if tag := field.Tag.Get("json"); tag == s.IDField {
				mongoID = val.Field(i)
			} else if tag == common.IDField {
				idField = val.Field(i)
			}
		}

		if mongoID.IsValid() && idField.IsValid() {
			idField.Set(mongoID)
			mongoID.Set(reflect.Zero(mongoID.Type()))
		}
		return nil
	}

	return fmt.Errorf("input must be a pointer to struct or a map")
}
func (s *MongoDBStore) Get(key string, store string, dst interface{}) error {
	collection := s.db.Collection(store)
	err := collection.FindOne(s.ctx, bson.M{s.IDField: key}).Decode(dst)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return common.ErrNotFound
		}
		return fmt.Errorf("failed to find document: %w", err)
	}
	s.SetID(dst)
	return err
}

func (s *MongoDBStore) Delete(key string, store string) error {
	collection := s.db.Collection(store)
	_, err := collection.DeleteOne(s.ctx, bson.M{s.IDField: key})
	if err != nil {
		return fmt.Errorf("failed to delete document: %w", err)
	}
	return nil
}

func (s *MongoDBStore) CreateDatabase() error {
	return nil
}

func (s *MongoDBStore) CreateTable(table string, sample interface{}) error {
	// collection := s.db.Collection(table)
	// // Check if collection exists. If not, create it.
	// if err := collection.Indexes().CreateOne(context.TODO(), mongo.IndexModel{}); err != nil && err != mongo.ErrCollectionNotFound {
	// 	return fmt.Errorf("failed to create collection: %w", err)
	// }

	// //Infer schema from sample if provided
	// if sample != nil {
	// 	sampleType := reflect.TypeOf(sample)
	// 	if sampleType.Kind() == reflect.Struct {
	// 		//Iterate through fields and create indexes as needed.  This is a basic example and may need refinement based on your specific needs.
	// 		for i := 0; i < sampleType.NumField(); i++ {
	// 			field := sampleType.Field(i)
	// 			if field.Tag.Get("index") == "true" {
	// 				indexModel := mongo.IndexModel{
	// 					Keys: bson.D{{field.Name, 1}}, // Ascending index
	// 				}
	// 				if err := collection.Indexes().CreateOne(context.TODO(), indexModel); err != nil {
	// 					return fmt.Errorf("failed to create index for field '%s': %w", field.Name, err)
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	return nil
}
func (s *MongoDBStore) Close() {
	if s.client != nil {
		s.client.Disconnect(s.ctx)
		log.Println("Disconnected from MongoDB")
	}
}

func (s *MongoDBStore) Before(id string, count int, skip int, store string) (prows common.ObjectRows, err error) {
	collection := s.db.Collection(store)
	// Assuming 'id' field represents a timestamp or comparable value
	cursor, err := collection.Find(s.ctx, bson.M{s.IDField: bson.M{"$lt": id}}, options.Find().SetLimit(int64(count)).SetSort(bson.D{{s.IDField, -1}}).SetSkip(int64(skip)))
	if err != nil {
		return nil, fmt.Errorf("failed to find documents before ID: %w", err)
	}
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, nil
}

// This will retrieve all new rows that were created since the row with id was created
// [1, 2, 3, 4], since 2 will return [3, 4]
func (s *MongoDBStore) Since(id string, count, skip int, store string) (prows common.ObjectRows, err error) {
	collection := s.db.Collection(store)
	// Assuming 'id' field represents a timestamp or comparable value
	cursor, err := collection.Find(s.ctx, bson.M{s.IDField: bson.M{"$gt": id}}, options.Find().SetLimit(int64(count)).SetSort(bson.D{{s.IDField, 1}}).SetSkip(int64(skip)))
	if err != nil {
		return nil, fmt.Errorf("failed to find documents since ID: %w", err)
	}
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, nil
}

func (s *MongoDBStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *MongoDBStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	collection := s.db.Collection(store)
	cursor, err := collection.Find(s.ctx, bson.M{}, options.Find().SetLimit(int64(count)).SetSkip(int64(skip)))
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, nil
}

func (s *MongoDBStore) AllCursor(store string) (common.ObjectRows, error) {
	collection := s.db.Collection(store)
	cursor, err := collection.Find(s.ctx, bson.M{}, options.Find())
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, err
}

func (s *MongoDBStore) GetAll(count int, skip int, store string) ([][][]byte, error) {
	rows, err := s.All(count, skip, store)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result [][][]byte
	for {
		raw, ok := rows.NextRaw()
		if !ok {
			if err := rows.LastError(); err != nil {
				return nil, err
			}
			break
		}
		result = append(result, [][]byte{raw})
	}
	return result, nil
}

func (s *MongoDBStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	processedFilter := s.processFilter(filter)
	collection := s.db.Collection(store)
	_, err := collection.UpdateMany(s.ctx, processedFilter, bson.M{"$set": src})
	if err != nil {
		return fmt.Errorf("failed to replace documents: %w", err)
	}
	return nil
}
func (s *MongoDBStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	processedFilter := s.processFilter(filter)
	collection := s.db.Collection(store)
	_, err := collection.UpdateMany(s.ctx, processedFilter, bson.M{"$set": src})
	if err != nil {
		return fmt.Errorf("failed to update documents: %w", err)
	}
	return nil
}
func (s *MongoDBStore) Filter(filter map[string]interface{}, count int, skip int, store string) (common.ObjectRows, error) {
	processedFilter := s.processFilter(filter)
	collection := s.db.Collection(store)
	cursor, err := collection.Find(s.ctx, processedFilter, options.Find().SetLimit(int64(count)).SetSkip(int64(skip)))
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, nil
}

func (s *MongoDBStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (rows common.ObjectRows, err error) {
	return nil, common.ErrNotImplemented
}

func (s *MongoDBStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	return 0, common.ErrNotImplemented
}

func (s *MongoDBStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (rows common.ObjectRows, err error) {
	return nil, common.ErrNotImplemented
}

func (s *MongoDBStore) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s *MongoDBStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s *MongoDBStore) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	collection := s.db.Collection(store)
	result, err := collection.InsertMany(s.ctx, data)
	if err != nil {
		return nil, fmt.Errorf("failed to batch insert documents: %w", err)
	}

	keys = make([]string, len(result.InsertedIDs))
	for i, id := range result.InsertedIDs {
		keys[i] = id.(primitive.ObjectID).Hex()
	}
	return keys, nil
}

func (s *MongoDBStore) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	processedFilter := s.processFilter(filter)
	collection := s.db.Collection(store)
	_, err = collection.DeleteMany(s.ctx, processedFilter)
	if err != nil {
		return fmt.Errorf("failed to delete documents: %w", err)
	}
	return nil
}

func (s *MongoDBStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}

func (s *MongoDBStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	processedFilter := s.processFilter(filter)
	collection := s.db.Collection(store)
	count, err := collection.CountDocuments(s.ctx, processedFilter)
	if err != nil {
		return 0, fmt.Errorf("failed to count filtered documents: %w", err)
	}
	return count, nil
}

func (s *MongoDBStore) Count(store string) (int, error) {
	collection := s.db.Collection(store)
	count, err := collection.CountDocuments(s.ctx, bson.M{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}
	return int(count), nil
}

func (s *MongoDBStore) Update(key string, store string, src interface{}) error {
	collection := s.db.Collection(store)
	filter := bson.M{s.IDField: key}
	update := bson.M{"$set": src}
	_, err := collection.UpdateOne(s.ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update document: %w", err)
	}
	return nil
}

func (s *MongoDBStore) DB() *mongo.Database {
	return s.db
}

func (s *MongoDBStore) GetStore() interface{} {
	return s.client
}

func (s *MongoDBStore) BeginTransaction() (common.Transaction, error) {
	session, err := s.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start session: %w", err)
	}
	txn := &MongoTransaction{session: session, ctx: s.ctx}
	return txn, nil
}

func (s *MongoDBStore) SaveAll(store string, srcArray ...interface{}) ([]string, error) {
	collection := s.db.Collection(store)
	var keys []string
	for _, src := range srcArray {
		res, err := collection.InsertOne(s.ctx, src)
		if err != nil {
			return nil, fmt.Errorf("failed to insert document: %w", err)
		}
		keys = append(keys, res.InsertedID.(primitive.ObjectID).String())
	}
	return keys, nil
}

func (s *MongoDBStore) processFilter(filter map[string]interface{}) map[string]interface{} {
	qCopy := make(map[string]interface{})
	if filter != nil {
		if q, ok := filter["q"].(map[string]interface{}); ok {
			for k, v := range q {
				qCopy[k] = v
			}
			if id, ok := qCopy[common.IDField]; ok {
				qCopy[s.IDField] = id
				delete(qCopy, common.IDField)
			}
		}
	}
	return qCopy
}

func (s *MongoDBStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) error {
	collection := s.db.Collection(store)
	processedFilter := s.processFilter(filter)
	query := processedFilter

	err := collection.FindOne(s.ctx, query).Decode(dst)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return common.ErrNotFound
		}
		return fmt.Errorf("failed to find document: %w", err)
	}
	return nil
}
func (s *MongoDBStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (prows common.ObjectRows, err error) {
	collection := s.db.Collection(store)
	findOpts := options.Find().SetLimit(int64(count)).SetSkip(int64(skip))

	processedFilter := s.processFilter(filter)
	query := processedFilter
	if geoQuery := opts.GetGeoQuery(); geoQuery != nil {
		if len(geoQuery.LocationField) > 0 {
			query["loc"] = bson.M{"$geoWithin": bson.M{"$geometry": geoQuery}}
		}
	}
	cursor, err := collection.Find(s.ctx, query, findOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	return &MongoRows{mongodb: s, cursor: cursor, ctx: s.ctx}, nil
}

func (s *MongoDBStore) GetByField(field string, value string, store string, dst interface{}) error {
	collection := s.db.Collection(store)
	err := collection.FindOne(s.ctx, bson.M{field: value}).Decode(dst)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return common.ErrNotFound
		}
		return fmt.Errorf("failed to find document: %w", err)
	}
	return nil
}

func (s *MongoDBStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) error {
	collection := s.db.Collection(store)
	query := bson.M{name: val} // Use name and val for querying
	projection := bson.M{}
	for _, field := range fields {
		projection[field] = 1 // Include field in projection
	}
	opts := options.FindOne().SetProjection(projection)
	err := collection.FindOne(s.ctx, query, opts).Decode(dst)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return common.ErrNotFound
		}
		return fmt.Errorf("failed to find document: %w", err)
	}
	return nil
}

func (s *MongoDBStore) Replace(key string, store string, src interface{}) error {
	collection := s.db.Collection(store)
	filter := bson.M{s.IDField: key}
	_, err := collection.ReplaceOne(s.ctx, filter, src)
	if err != nil {
		return fmt.Errorf("failed to replace document: %w", err)
	}
	return nil
}

func (s *MongoDBStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	processedFilter := s.processFilter(filter)
	rows, err := s.FilterGetAll(processedFilter, count, skip, store, opts)
	return rows, common.AggregateResult{}, err
}

func (s *MongoDBStore) Stats(store string) (map[string]interface{}, error) {
	count, err := s.Count(store)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"count": count}, nil
}

func New(ctx context.Context, uri string, dbName string) (*MongoDBStore, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return &MongoDBStore{client: client, ctx: ctx, dbName: dbName,
		createdAt: time.Now(),
		db:        client.Database(dbName),
		IDField:   "_id", // Set a default idKey
	}, nil
}
