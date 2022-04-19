package query

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"

	"github.com/jinzhu/gorm"
	uuid "github.com/satori/go.uuid"
	"github.com/srostyslav/file"
)

type SqlQuery struct {
	fileName, query string
	db              *gorm.DB
	params          []interface{}

	rows        *sql.Rows
	total       int
	columns     []string
	length      int
	Error       error
	initialized bool
	parseByte   bool
}

func (q *SqlQuery) init() error {
	if q.initialized {
		return q.Error
	}
	if q.query == "" {
		q.query, q.Error = (&file.File{Name: q.fileName}).Content()
	}

	return q.Error
}

func (q *SqlQuery) setRows() error {
	if q.initialized {
		return q.Error
	}
	q.initialized = true

	if q.rows, q.Error = q.db.Raw(q.query, q.params...).Rows(); q.Error != nil {
		return q.Error
	}

	if q.columns, q.Error = q.rows.Columns(); q.Error != nil {
		return q.Error
	}

	q.length = len(q.columns)

	return nil
}

func (q *SqlQuery) scanRowToMap() (map[string]interface{}, error) {
	current, value := q.makeResultReceiver(), map[string]interface{}{}

	if q.Error = q.rows.Scan(current...); q.Error != nil {
		return value, q.Error
	}

	for i := 0; i < q.length; i++ {
		k := q.columns[i]
		val := *(current[i]).(*interface{})

		if q.parseByte {
			switch v := val.(type) {
			case []byte:

				if v != nil {
					value[k] = val
					continue
				} else if u, err := uuid.FromString(string(v)); err == nil {
					value[k] = u.String()
					continue
				}

				var f float64
				if json.Unmarshal(v, &f) == nil {
					value[k] = f
				}

				var i interface{}
				if json.Unmarshal(v, &i) == nil {
					value[k] = f
				}
			}
		}

		value[k] = val
	}

	return value, nil
}

func (q *SqlQuery) makeResultReceiver() []interface{} {
	result := make([]interface{}, 0, q.length)
	for i := 0; i < q.length; i++ {
		var current interface{} = struct{}{}
		result = append(result, &current)
	}
	return result
}

func (q *SqlQuery) Fetch(obj interface{}) (bool, error) {
	if q.Error = q.init(); q.Error != nil {
		return false, q.Error
	} else if q.Error = q.setRows(); q.Error != nil {
		return false, q.Error
	}

	if next := q.rows.Next(); next {
		switch v := obj.(type) {
		case *map[string]interface{}:
			if *v, q.Error = q.scanRowToMap(); q.Error != nil {
				q.rows.Close()
				return false, q.Error
			}
		default:
			if q.Error = q.db.ScanRows(q.rows, obj); q.Error != nil {
				q.rows.Close()
				return false, q.Error
			}
		}
		q.total++
		return next, nil
	} else {
		return next, q.rows.Close()
	}
}

func (q *SqlQuery) fetchAll(obj interface{}) ([]interface{}, error) {
	var (
		next bool
		list = []interface{}{}
	)

	for next, q.Error = q.Fetch(obj); q.Error == nil && next; next, q.Error = q.Fetch(obj) {
		list = append(list, reflect.ValueOf(obj).Elem().Interface())
	}

	return list, q.Error
}

func (q *SqlQuery) ToList() ([]map[string]interface{}, error) {
	var (
		result = []map[string]interface{}{}
		rows   = []interface{}{}
	)

	if rows, q.Error = q.fetchAll(&map[string]interface{}{}); q.Error != nil {
		return result, q.Error
	} else {
		for _, item := range rows {
			result = append(result, item.(map[string]interface{}))
		}
		return result, nil
	}
}

func (q *SqlQuery) First(obj interface{}) error {

	var next bool
	if next, q.Error = q.Fetch(obj); q.Error != nil {
		return q.Error
	} else if !next {
		return errors.New("record not found")
	}
	q.rows.Close()
	return nil
}

func (q *SqlQuery) Scan(obj interface{}) error {
	if q.Error = q.init(); q.Error != nil {
		return q.Error
	}

	q.Error = q.db.Raw(q.query, q.params...).Scan(obj).Error
	return q.Error
}

func (q *SqlQuery) Write(w http.ResponseWriter, start, end string, obj interface{}) error {

	if start == "" {
		start = "["
	}
	if end == "" {
		end = "]"
	}

	w.Write([]byte(start))

	var (
		next bool
		out  []byte
	)
	for next, q.Error = q.Fetch(obj); q.Error == nil && next; next, q.Error = q.Fetch(obj) {
		if out, q.Error = json.Marshal(obj); q.Error != nil {
			return q.Error
		} else {
			if q.total > 1 {
				w.Write([]byte(","))
			}
			w.Write(out)
		}
	}
	w.Write([]byte(end))
	return q.Error
}

func (q *SqlQuery) Total() int {
	return q.total
}

func NewSqlFromFile(fileName string, parseByte bool, db *gorm.DB, params ...interface{}) *SqlQuery {
	return &SqlQuery{fileName: fileName, db: db, params: params, parseByte: parseByte}
}

func NewSql(query string, parseByte bool, db *gorm.DB, params ...interface{}) *SqlQuery {
	return &SqlQuery{query: query, db: db, params: params, parseByte: parseByte}
}
