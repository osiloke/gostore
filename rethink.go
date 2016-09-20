package gostore

import (
	r "github.com/dancannon/gorethink"
	"github.com/jinzhu/now"
	"github.com/mgutz/logxi/v1"
	"strings"
	"time"
)

var logger = log.New("gostore.rethink")

func NewRethinkObjectStoreAndSession(address, database string) (RethinkStore, error) {
	session, err := r.Connect(r.ConnectOpts{
		Address: address,
		// MaxIdle: 10,
		// MaxOpen: 10,
		// Timeout: time.Second * 100,
	})
	//			r.SetVerbose(true)
	if err != nil {
		println("Error while opening rethinkdb database", err.Error())
		return RethinkStore{}, err
	}
	return NewRethinkObjectStore(session, database), nil
}

func NewRethinkObjectStore(session *r.Session, database string) RethinkStore {
	s := RethinkStore{session, database}
	s.CreateDatabase()
	return s
}

func NewMockRethinkObjectStore(mock *r.Mock, database string) RethinkStore {
	s := RethinkStore{mock, database}
	s.CreateDatabase()
	return s
}

type RethinkStore struct {
	Session  r.QueryExecutor
	Database string
}

type RethinkRows struct {
	cursor *r.Cursor
}

func (s RethinkRows) LastError() error {
	return nil
}

func (s RethinkRows) Next(dst interface{}) (bool, error) {
	if !s.cursor.Next(dst) {
		//		logger.Debug("Error getting next", "err", s.cursor.Err(), "isNil", s.cursor.IsNil())
		return false, s.cursor.Err()
	}
	return true, nil
}

func (s RethinkRows) Close() {
	s.cursor.Close()
}

func NewRethinkRows(cursor *r.Cursor) RethinkRows {
	return RethinkRows{cursor}
}

func (s RethinkStore) CreateDatabase() (err error) {
	return r.DBCreate(s.Database).Exec(s.Session)
}

func (s RethinkStore) GetStore() interface{} {
	return s.Session
}

func hasIndex(name string, indexes []interface{}) bool {
	for _, v := range indexes {
		if v.(string) == name {
			return true
		}
	}
	return false
}

func (rs RethinkStore) DropTable(store string) error {
	return r.DB(rs.Database).TableDrop(store).Exec(rs.Session)
}

//TODO: fix index creation, indexes are not created properly
func (rs RethinkStore) CreateTable(store string, schema interface{}) (err error) {
	logger.Info("creating table " + store)
	var res []interface{}
	_ = r.DB(rs.Database).TableCreate(store).Exec(rs.Session)
	// if err != nil{
	// 	// logger.Warn("unable to create table ", "table", store)
	//
	// }
	result, err := r.DB(rs.Database).Table(store).IndexList().Run(rs.Session)
	if err != nil {
		panic(err)
	}
	if result.Err() != nil {
		panic(err)
	}
	result.All(&res)
	logger.Debug(store+" indexList", res)
	//also create indexes
	if schema != nil {
		s := schema.(map[string]interface{})
		if indexes, ok := s["index"].(map[string]interface{}); ok {
			for name, _vals := range indexes {
				if !hasIndex(name, res) {
					logger.Info("creating index", "name", name, "val", _vals)
					if vals, ok := _vals.([]interface{}); ok && len(vals) > 0 {
						logger.Info("creating compound index", "name", name, "vals", vals)
						if _, err = r.DB(rs.Database).Table(store).IndexCreateFunc(name, func(row r.Term) interface{} {
							index_fields := []interface{}{}
							for _, v := range vals {
								index_fields = append(index_fields, row.Field(v.(string)))
							}
							return index_fields
						}).RunWrite(rs.Session); err != nil {
							return
						}
					} else {
						logger.Info("creating index", "name", name)
						if err = r.DB(rs.Database).Table(store).IndexCreate(name).Exec(rs.Session); err != nil {
							// logger.Warn("cannot create index [" + name + "] in " + store)
							// logger.Warn("cannot create index")
							println(err.Error())

						} else {
							if err = r.DB(rs.Database).Table(store).IndexWait(name).Exec(rs.Session); err == nil {
								logger.Info("created index [" + name + "] in " + store)
							}
						}
					}
				}
			}
		}
	}

	return
}

type TermOperators map[string]func(args ...interface{}) r.Term

func parseFilter(args string) (dtarg interface{}) {
	//this also handles args
	vals := strings.Split(args, "|")
	if len(vals) > 1 {
		//check type
		if vals[1] == "dt" {
			if it, err := ToInt(vals[0]); err == nil {
				logger.Info("< op date", "time", it)
				return r.EpochTime(it)
			}
			//				t, err := now.Parse(vals[0])
			if t, err := time.Parse(
				time.RFC3339,
				vals[0]); err == nil {
				return r.EpochTime(t.Unix())
			}
			if t, err := now.Parse(vals[0]); err == nil {
				return r.EpochTime(t.Unix())
			}
		}
	}
	return vals[0]
}

// func handleOrArgs

//filterOps returns rethink term operators which filter a table
//Currently supported filters:
// ~<query>:performs a fuzzy match against a query. query is any valid regex
// ><value>:performs a greater than query
// <<value>:performs a less than query
var filterOps TermOperators = TermOperators{
	"=": func(args ...interface{}) r.Term {
		var baseTerm r.Term
		fieldName := args[0].(string)
		vals := strings.Split(args[2].(string), "|")
		if len(vals) > 0 {
			baseTerm = r.Row.Field(fieldName).Eq(vals[0])
			for _, v := range vals[1:] {
				baseTerm = orTerm(baseTerm, r.Row.Field(fieldName).Eq(v))
			}
		} else {
			baseTerm = baseTerm.Match(vals[0])
		}
		return baseTerm
	},
	"~": func(args ...interface{}) r.Term {
		var baseTerm r.Term
		fieldName := args[0].(string)
		vals := strings.Split(args[2].(string), "|")
		if len(vals) > 0 {
			baseTerm = r.Row.Field(fieldName).Match(vals[0])
			for _, v := range vals[1:] {
				baseTerm = orTerm(baseTerm, r.Row.Field(fieldName).Match(v))
			}
		} else {
			baseTerm = baseTerm.Match(vals[0])
		}
		return baseTerm
	},
	">": func(args ...interface{}) r.Term {
		baseTerm := args[1].(r.Term)
		return baseTerm.Gt(parseFilter(args[1].(string)))
	},
	"<": func(args ...interface{}) r.Term {
		baseTerm := args[1].(r.Term)
		return baseTerm.Lt(parseFilter(args[1].(string)))
	},
}

func orTerm(baseTerm r.Term, to r.Term) r.Term {
	return baseTerm.Or(to)
}

func andTerm(baseTerm r.Term, to r.Term) r.Term {
	return baseTerm.And(to)
}

func (s RethinkStore) parseFilterOpsTerm(key, val string) (t r.Term) {
	var start int = 1
	val_rune := []rune(val)
	first := string(val_rune[0])
	if op, ok := filterOps[first]; ok {
		tv := string([]rune(val)[start:])
		t = op(key, r.Row.Field(key), tv)
	} else {
		t = r.Row.Field(key).Eq(val)
	}
	return
}
func (s RethinkStore) transformFilter(rootTerm interface{}, filter map[string]interface{}) (f r.Term) {
	var _root interface{}
	if rootTerm != nil {
		_root = rootTerm
		f = _root.(r.Term)
	}
	for fieldKey, fieldVal := range filter {
		if subFilter, ok := fieldVal.(map[string]interface{}); ok {
			if fieldKey == "or" {
				f = f.Or(s.transformFilter(rootTerm, subFilter))
			} else {
				f = f.And(s.transformFilter(rootTerm, subFilter))
			}
		} else if subFilterGroup, ok := fieldVal.([]interface{}); ok {

			terms := make([]interface{}, 0, len(subFilterGroup))
			for _, element := range subFilterGroup {
				terms = append(terms, s.transformFilter(nil, element.(map[string]interface{})))
			}
			if fieldKey == "or" {
				f = f.Or(terms...)
			} else {

				f = f.And(terms...)
			}
		} else {
			if _root != nil {
				f = f.And(s.parseFilterOpsTerm(fieldKey, fieldVal.(string)))
			} else {
				_root = s.parseFilterOpsTerm(fieldKey, fieldVal.(string))
				f = _root.(r.Term)
			}
		}
	}

	return
}
func (s RethinkStore) filterTerm(filter map[string]interface{}, opts ObjectStoreOptions, args ...interface{}) (filterTerm r.Term) {
	// var hasIndex = false
	// var hasMultiIndex = false
	// var indexName string
	// var indexVal string

	// if opts != nil {
	// 	indexes := opts.GetIndexes()
	// 	for k, v := range filter {

	// 	}

	// }
	return
}

// http://stackoverflow.com/questions/19747207/rethinkdb-index-for-filter-orderby
//TODO: fix index selection, it should favour compound indexes more
func (s RethinkStore) getRootTerm(store string, filter map[string]interface{}, opts ObjectStoreOptions, args ...interface{}) (rootTerm r.Term) {
	rootTerm = r.DB(s.Database).Table(store)
	var hasIndex = false
	var hasMultiIndex = false
	var indexName string
	var indexVal string
	if opts != nil {
		indexes := opts.GetIndexes()
		for name := range indexes {
			if val, ok := filter[name].(string); ok {
				hasIndex = true
				indexVal = val
				indexName = name
				ix_id_name := name + "_id"
				if _, ok := indexes[ix_id_name]; ok {
					indexName = ix_id_name
					hasMultiIndex = true
					break
				}
				break
			}
		}
	}
	if !hasIndex {
		if len(args) == 0 {
			rootTerm = rootTerm.OrderBy(
				r.OrderByOpts{Index: r.Desc("id")})
		}
	} else {
		if hasMultiIndex {
			rootTerm = rootTerm.Between(
				[]interface{}{indexVal, r.MinVal},
				[]interface{}{indexVal, r.MaxVal},
				r.BetweenOpts{Index: indexName, RightBound: "closed"}).OrderBy(r.OrderByOpts{Index: r.Desc(indexName)})
		} else {
			rootTerm = rootTerm.GetAllByIndex(indexName, indexVal)
		}
	}
	if len(filter) > 0 {
		rootTerm = rootTerm.Filter(s.transformFilter(nil, filter))
	}
	return
}

func (s RethinkStore) All(count int, skip int, store string) (rrows ObjectRows, err error) {
	term := r.DB(s.Database).Table(store).OrderBy(r.OrderByOpts{Index: r.Desc("id")})
	result, err := term.Run(s.Session)
	if err != nil {
		return
	}
	rrows = RethinkRows{result}
	return
}

func (s RethinkStore) AllCursor(store string) (ObjectRows, error) {
	result, err := r.DB(s.Database).Table(store).Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	return RethinkRows{result}, nil
}

func (s RethinkStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rrows ObjectRows, err error) {

	var rootTerm = s.getRootTerm(store, filter, opts)
	if count > 0 {
		rootTerm = rootTerm.Limit(count)
	}
	result, err := rootTerm.Skip(skip).Run(s.Session)
	if err != nil {
		logger.Error("err", "err", err)
		return
	}
	if result.IsNil() {
		return nil, ErrNotFound
	}
	rrows = RethinkRows{result}
	logger.Debug("AllWithinRange::done", "query", rootTerm.String(), "err", result.Err())
	return
}

//Before will retrieve all old rows that were created before the row with id was created
// [1, 2, 3, 4], before 2 will return [3, 4]
//r.db('worksmart').table('store').orderBy({'index': r.desc('id')}).filter(r.row('schemas')
// .eq('osiloke_tsaboin_silverbird').and(r.row('id').lt('55b54e93f112a16514000057')))
// .pluck('schemas', 'id','tid', 'timestamp', 'created_at').limit(100)
func (s RethinkStore) Before(id string, count int, skip int, store string) (rows ObjectRows, err error) {
	result, err := r.DB(s.Database).Table(store).Filter(r.Row.Field("id").Lt(id)).Limit(count).Skip(skip).Run(s.Session)
	if err != nil {
		return
	}
	defer result.Close()
	//	result.All(dst)
	rows = RethinkRows{result}
	return
}

//This will retrieve all new rows that were created since the row with id was created
// [1, 2, 3, 4], since 2 will return [1]
func (s RethinkStore) Since(id string, count, skip int, store string) (rrows ObjectRows, err error) {
	result, err := r.DB(s.Database).Table(store).Filter(r.Row.Field("id").Gt(id)).Limit(count).Skip(skip).Run(s.Session)
	if err != nil {
		return
	}
	//	result.All(dst)
	rrows = RethinkRows{result}
	return
}

func (s RethinkStore) Get(id, store string, dst interface{}) (err error) {
	var rootTerm = r.DB(s.Database).Table(store)
	result, err := rootTerm.Get(id).Run(s.Session)
	if err != nil {
		//		logger.Error("Get", "err", err)
		return
	}
	defer result.Close()
	if result.Err() != nil {
		return result.Err()
	}

	logger.Debug("Get", "key", id, "query", rootTerm.String())
	if result.IsNil() {
		return ErrNotFound
	}
	if err = result.One(dst); err == r.ErrEmptyResult {
		//		logger.Error("Get", "err", err)
		return ErrNotFound
	}
	return nil
}

func (s RethinkStore) Save(store string, src interface{}) (key string, err error) {
	result, err := r.DB(s.Database).Table(store).Insert(src, r.InsertOpts{Durability: "soft"}).RunWrite(s.Session)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate primary key") {
			err = ErrDuplicatePk
		}
		return
	}
	if len(result.GeneratedKeys) > 0 {
		key = result.GeneratedKeys[0]
	}
	return

}

func (s RethinkStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	result, err := r.DB(s.Database).Table(store).Insert(src, r.InsertOpts{Durability: "hard"}).RunWrite(s.Session)
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate primary key") {
			err = ErrDuplicatePk
		}
		return
	}
	if len(result.GeneratedKeys) > 0 {
		keys = result.GeneratedKeys
	}
	return

}

func (s RethinkStore) Update(id string, store string, src interface{}) (err error) {
	_, err = r.DB(s.Database).Table(store).Get(id).Update(src, r.UpdateOpts{Durability: "soft"}).RunWrite(s.Session)
	return

}

func (s RethinkStore) Replace(id string, store string, src interface{}) (err error) {
	_, err = r.DB(s.Database).Table(store).Get(id).Replace(src, r.ReplaceOpts{Durability: "soft"}).RunWrite(s.Session)
	return
}

func (s RethinkStore) Delete(id string, store string) (err error) {
	_, err = r.DB(s.Database).Table(store).Get(id).Delete(r.DeleteOpts{Durability: "hard"}).RunWrite(s.Session)
	return
}
func (s RethinkStore) DeleteAll(store string) (err error) {
	_, err = r.DB(s.Database).Table(store).Delete(r.DeleteOpts{Durability: "hard"}).RunWrite(s.Session)
	return
}

func (s RethinkStore) Stats(store string) (map[string]interface{}, error) {
	result, err := r.DB(s.Database).Table(store).Count().Run(s.Session)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	var cnt int64
	if err = result.One(&cnt); err != nil {
		return nil, ErrNotFound
	}
	return map[string]interface{}{"count": cnt}, nil
}

func (s RethinkStore) GetByField(name, val, store string, dst interface{}) (err error) {
	result, err := r.DB(s.Database).Table(store).Filter(r.Row.Field(name).Eq(val)).Run(s.Session)
	if err != nil {
		return
	}
	defer result.Close()
	if err = result.One(dst); err == r.ErrEmptyResult {
		return ErrNotFound
	}
	return
}

//FilterBefore returns rows created before a provided key. It accepts a filter and result shaping arguments
func (s RethinkStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	rootTerm := r.DB(s.Database).Table(store).Between(
		r.MinVal, id, r.BetweenOpts{RightBound: "closed"}).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.transformFilter(nil, filter)).Limit(count)
	result, err := rootTerm.Run(s.Session)
	if err != nil {
		return
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	if result.IsNil() {
		return nil, ErrNotFound
	}
	rows = RethinkRows{result}
	return
}

func (s RethinkStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	result, err := r.DB(s.Database).Table(store).Between(
		r.MinVal, id).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.transformFilter(nil, filter)).Count().Run(s.Session)
	defer result.Close()

	var cnt int64
	if err = result.One(&cnt); err != nil {
		return 0, ErrNotFound
	}
	return cnt, nil
}

func (s RethinkStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	result, err := r.DB(s.Database).Table(store).Between(
		id, r.MaxVal, r.BetweenOpts{LeftBound: "open", Index: "id"}).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.transformFilter(nil, filter)).Limit(count).Run(s.Session)
	if err != nil {
		return
	}
	if result.Err() != nil {
		return nil, result.Err()
	}
	if result.IsNil() == true {
		return nil, ErrNotFound
	}
	rows = RethinkRows{result}
	return
}
func (s RethinkStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) (err error) {
	_, err = r.DB(s.Database).Table(store).Update(src, r.UpdateOpts{Durability: "soft"}).RunWrite(s.Session)
	return
}

func (s RethinkStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) (err error) {
	_, err = r.DB(s.Database).Table(store).Replace(src, r.ReplaceOpts{Durability: "soft"}).RunWrite(s.Session)
	return
}

/*
FilterGet retrieves only one item based on a filter
It is used as a shortcut to FilterGetAll with size == 1
*/
func (s RethinkStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {

	// var rootTerm = s.getRootTerm(store, filter, opts)
	var rootTerm = r.DB(s.Database).Table(store)
	if opts != nil {
		indexes := opts.GetIndexes()
		for k := range indexes {
			if val, ok := filter[k].(string); ok {
				rootTerm = rootTerm.GetAllByIndex(k, val)
				break
			}
		}
	}
	rootTerm = rootTerm.Filter(s.transformFilter(nil, filter))
	result, err := rootTerm.Limit(1).Run(s.Session)
	logger.Debug("FilterGet::done", "store", store, "query", rootTerm.String())
	if err != nil {
		logger.Error("failed to get", "err", err.Error())
		return err
	}
	defer result.Close()
	if result.Err() != nil {
		return result.Err()
	}
	if result.IsNil() == true {
		return ErrNotFound
	}
	if err := result.One(dst); err != nil {
		println(err.Error())
		if err == r.ErrEmptyResult {
			return ErrNotFound
		} else {
			return err
		}

	}
	return nil
}

func (s RethinkStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rrows ObjectRows, err error) {

	var rootTerm = s.getRootTerm(store, filter, opts)
	var query r.Term
	if skip == 0 && count+skip == 0 {
		query = rootTerm
	} else {
		query = rootTerm.Slice(skip, count+skip)
	}
	result, err := query.Run(s.Session)
	if err != nil {
		logger.Error("err", "err", err)
		return
	}
	logger.Debug("FilterGetAll::done", "query", rootTerm, "store", store)
	if result.Err() != nil {
		return nil, result.Err()
	}
	if result.IsNil() {
		return nil, ErrNotFound
	}
	rrows = RethinkRows{result}
	return
}

func (s RethinkStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) (err error) {
	_ = "breakpoint"
	_ = "FilterDelete"
	var rootTerm = s.getRootTerm(store, filter, opts)
	_, err = rootTerm.Delete(r.DeleteOpts{Durability: "hard"}).RunWrite(s.Session)
	if err == r.ErrEmptyResult {
		return ErrNotFound
	}
	return
}
func (s RethinkStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts ObjectStoreOptions) (err error) {
	terms := make([]interface{}, 2)
	for i, f := range filter {
		var term = s.getRootTerm(store, f, opts)
		terms[i] = term
	}
	rootTerm := r.Union(terms...).Delete()
	_, err = rootTerm.RunWrite(s.Session)
	if err == r.ErrEmptyResult {
		return ErrNotFound
	}

	return
}

func (s RethinkStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	_ = "breakpoint"
	_ = "FilterCount"
	var rootTerm = s.getRootTerm(store, filter, opts)
	result, err := rootTerm.Count().Run(s.Session)
	if err != nil {
		return 0, err
	}
	defer result.Close()
	if result.Err() != nil {
		return 0, result.Err()
	}
	var cnt int64
	// logger.Debug("FilterCount", "query", rootTerm.String(), "res", result)
	if err = result.One(&cnt); err != nil {
		return 0, ErrNotFound
	}
	return cnt, nil
}

func (s RethinkStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	result, err := r.DB(s.Database).Table(store).Filter(r.Row.Field(name).Eq(val)).Pluck(fields).Run(s.Session)
	if err != nil {
		return
	}
	defer result.Close()
	if err = result.One(dst); err == r.ErrEmptyResult {
		return ErrNotFound
	}
	return
}

func (s RethinkStore) Close() {
	s.Session.(*r.Session).Close()
}
