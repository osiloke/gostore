package gostore

import (
	"fmt"
	"github.com/asaskevich/govalidator"
	r "github.com/dancannon/gorethink"
	"github.com/dustin/gojson"
	"github.com/jinzhu/now"
	"github.com/mgutz/logxi/v1"
	"strings"
	"time"
)

var logger = log.New("gostore.rethink")

func NewRethinkObjectStore(session *r.Session, database string) RethinkStore {
	s := RethinkStore{session, database}
	s.CreateDatabase()
	return s
}

type RethinkStore struct {
	Session  *r.Session
	Database string
}

type RethinkRows struct {
	cursor *r.Cursor
}

func (s RethinkRows) LastError() error {
	return nil
}
func NewRethinkRows(cursor *r.Cursor) RethinkRows {
	return RethinkRows{cursor}
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
			if it, err := govalidator.ToInt(vals[0]); err == nil {
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

var filterOps TermOperators = TermOperators{
	"~": func(args ...interface{}) r.Term {
		vals := strings.Split(args[1].(string), "|")
		baseTerm := args[0].(r.Term)
		if len(vals) > 0 {

		}
		return baseTerm.Match(vals[0])
	},
	">": func(args ...interface{}) r.Term {
		baseTerm := args[0].(r.Term)
		return baseTerm.Gt(parseFilter(args[1].(string)))
	},
	"<": func(args ...interface{}) r.Term {
		baseTerm := args[0].(r.Term)
		return baseTerm.Lt(parseFilter(args[1].(string)))
	},
}

func orTerm(baseTerm r.Term, to r.Term) r.Term {
	return baseTerm.Or(to)
}

func andTerm(baseTerm r.Term, to r.Term) r.Term {
	return baseTerm.And(to)
}

// TODO: Transforms filter args into rethinkdb filter args
func (s RethinkStore) ParseFilterArgs(filter map[string]interface{}, indexes []string, opts ObjectStoreOptions) r.Term {
	var (
		t r.Term = r.And(1)
	)
	logger.Debug("filterArgs", "filter", filter, "indexes", indexes, "opts", opts)
	//TODO: Optimize this by passing indexes as well as field types
	orGroup := map[string]r.Term{}
	for k, v := range filter {
		val := v.(string)
		key_rune := []rune(k)
		val_rune := []rune(val)

		if len(val_rune) > 0 {
			var start int = 1
			// joinMethod := andTerm
			first := string(val_rune[0])
			//Handle `or` grouping by key
			if string(key_rune[0]) == "|" {
				//split |groupName|field_name
				orGroupName := ""
				k_index := 0
				rune_val := ""
				for _rune_index, v := range key_rune[1:] {
					rune_val = string(v)
					if rune_val == "|" {
						k_index = _rune_index + 2
						break
					}
					orGroupName += rune_val
				}
				k = string(key_rune[k_index:])
				if op, ok := filterOps[first]; ok {
					tv := string([]rune(val)[start:])
					if orGroupVal, ok := orGroup[orGroupName]; !ok {
						orGroup[orGroupName] = op(r.Row.Field(k), tv)
					} else {
						orGroup[orGroupName] = orGroupVal.Or(op(r.Row.Field(k), tv))
					}
				} else {
					if orGroupVal, ok := orGroup[orGroupName]; !ok {
						orGroup[orGroupName] = r.Row.Field(k).Eq(v)
					} else {
						orGroup[orGroupName] = orGroupVal.Or(r.Row.Field(k).Eq(v))
					}
				}
			} else {
				if op, ok := filterOps[first]; ok {
					tv := string([]rune(val)[start:])
					t = t.And(op(r.Row.Field(k), tv))
				} else {
					t = t.And(r.Row.Field(k).Eq(v))
				}
			}

		}
	}
	for _, v := range orGroup {
		t = t.And(v)
	}
	// logger.Debug(t.String())
	return t
}

func (s RethinkStore) All(count int, skip int, store string) (rrows ObjectRows, err error) {
	result, err := r.DB(s.Database).Table(store).OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Run(s.Session)
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
	logger.Debug("deleting " + id + " from " + store)
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

// http://stackoverflow.com/questions/19747207/rethinkdb-index-for-filter-orderby
func (s RethinkStore) getRootTerm(store string, filter map[string]interface{}, opts ObjectStoreOptions) (rootTerm r.Term) {
	rootTerm = r.DB(s.Database).Table(store)
	indexes := opts.GetIndexes()
	logger.Debug("getRootTerm", "store", store, "filter", filter, "opts", opts)
	var hasIndex = false
	var hasMultiIndex = false
	var indexName string
	var indexVal string
	for name := range indexes {
		logger.Debug("checking index " + name)
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
	if !hasIndex {
		rootTerm = rootTerm.OrderBy(
			r.OrderByOpts{Index: r.Desc("id")})
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
	rootTerm = rootTerm.Filter(s.ParseFilterArgs(filter, nil, opts))
	return
}
func (s RethinkStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	_ = "breakpoint"
	result, err := r.DB(s.Database).Table(store).Between(
		r.MinVal, id, r.BetweenOpts{RightBound: "closed"}).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.ParseFilterArgs(filter, nil, opts)).Limit(count).Run(s.Session)
	if err != nil {
		return
	}
	//	var dst interface{}
	f, _ := json.Marshal(filter)
	logger.Debug("FilterBefore", "query",
		fmt.Sprintf("r.db('%s').table('%s').between(r.minval, '%s').orderBy({index:r.desc('id')}).filter(%s).limit(%d)",
			s.Database, store, id, string(f), count))
	rows = RethinkRows{result}
	return
}

func (s RethinkStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	_ = "breakpoint"
	_ = "FilterGet"
	result, err := r.DB(s.Database).Table(store).Between(
		r.MinVal, id).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.ParseFilterArgs(filter, nil, opts)).Count().Run(s.Session)
	defer result.Close()

	var cnt int64
	if err = result.One(&cnt); err != nil {
		return 0, ErrNotFound
	}
	return cnt, nil
}

func (s RethinkStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	_ = "breakpoint"
	_ = "FilterGet"
	result, err := r.DB(s.Database).Table(store).Between(
		id, r.MaxVal, r.BetweenOpts{LeftBound: "open", Index: "id"}).OrderBy(
		r.OrderByOpts{Index: r.Desc("id")}).Filter(
		s.ParseFilterArgs(filter, nil, opts)).Limit(count).Run(s.Session)
	if err != nil {
		return
	}
	//	result.All(dst)
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

func (s RethinkStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {

	// var rootTerm = s.getRootTerm(store, filter, opts)
	var rootTerm = r.DB(s.Database).Table(store)
	indexes := opts.GetIndexes()
	for k := range indexes {
		if val, ok := filter[k].(string); ok {
			rootTerm = rootTerm.GetAllByIndex(k, val)
			break
		}
	}
	rootTerm = rootTerm.Filter(s.ParseFilterArgs(filter, nil, opts))
	result, err := rootTerm.Limit(1).Run(s.Session)
	logger.Debug("FilterGet::done", "store", store, "query", rootTerm.String())
	// logger.Debug("filter get", "opts", opts, "filter", filter, "query", rootTerm.String())
	if err != nil {
		logger.Error("failed to get", "err", err.Error())
		return err
	}
	defer result.Close()
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

	logger.Debug("FilterGetAll::start", "filter", filter, "count", count, "skip", skip, "store", store)
	var rootTerm = s.getRootTerm(store, filter, opts)
	// if count > 0 {
	// 	rootTerm = rootTerm.Limit(count)
	// }
	// result, err := rootTerm.Skip(skip).Run(s.Session)
	// if err != nil {
	// 	logger.Error("err", "err", err)
	// 	return
	// }
	query := rootTerm.Slice(skip, count+skip)
	result, err := query.Run(s.Session)
	if err != nil {
		logger.Error("err", "err", err)
		return
	}
	logger.Debug("FilterGetAll::done", "query", rootTerm, "err", result.Err(), "store", store)
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
	_, err = rootTerm.Delete(r.DeleteOpts{Durability: "soft"}).RunWrite(s.Session)
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
	var cnt int64
	if err = result.One(&cnt); err != nil {
		return 0, ErrNotFound
	}
	logger.Debug("FilterCount", "query", rootTerm.String())
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
	s.Session.Close()
}
