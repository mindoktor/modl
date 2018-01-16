package modl

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"context"
)

var _ = log.Fatal

type Invoice struct {
	ID       int64
	Created  int64 `db:"date_created"`
	Updated  int64
	Memo     string
	PersonID int64
	IsPaid   bool
}

type Person struct {
	ID      int64
	Created int64
	Updated int64
	FName   string
	LName   string
	Version int64
}

type InvoicePersonView struct {
	InvoiceID     int64
	PersonID      int64
	Memo          string
	FName         string
	LegacyVersion int64
}

type TableWithNull struct {
	ID      int64
	Str     sql.NullString
	Int64   sql.NullInt64
	Float64 sql.NullFloat64
	Bool    sql.NullBool
	Bytes   []byte
}

type WithIgnoredColumn struct {
	internal int64 `db:"-"`
	ID       int64
	Created  int64
}

type WithStringPk struct {
	ID   string
	Name string
}

type CustomStringType string

func (p *Person) PreInsert(ctx context.Context, s SqlExecutor) error {
	p.Created = time.Now().UnixNano()
	p.Updated = p.Created
	if p.FName == "badname" {
		return fmt.Errorf("invalid name: %s", p.FName)
	}
	return nil
}

func (p *Person) PostInsert(ctx context.Context, s SqlExecutor) error {
	p.LName = "postinsert"
	return nil
}

func (p *Person) PreUpdate(ctx context.Context, s SqlExecutor) error {
	p.FName = "preupdate"
	return nil
}

func (p *Person) PostUpdate(ctx context.Context, s SqlExecutor) error {
	p.LName = "postupdate"
	return nil
}

func (p *Person) PreDelete(ctx context.Context, s SqlExecutor) error {
	p.FName = "predelete"
	return nil
}

func (p *Person) PostDelete(ctx context.Context, s SqlExecutor) error {
	p.LName = "postdelete"
	return nil
}

func (p *Person) PostGet(ctx context.Context, s SqlExecutor) error {
	p.LName = "postget"
	return nil
}

type PersistentUser struct {
	Key            int32 `db:"mykey"`
	ID             string
	PassedTraining bool
}

func TestCreateTablesIfNotExists(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	err := dbmap.CreateTablesIfNotExists(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestPersistentUser(t *testing.T) {
	ctx := context.Background()
	dbmap := newDbMap()
	dbmap.ExecContext(ctx, "drop table if exists persistentuser")
	if len(os.Getenv("MODL_TEST_TRACE")) > 0 {
		dbmap.TraceOn("test", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	}
	dbmap.AddTable(PersistentUser{}).SetKeys(false, "mykey")
	err := dbmap.CreateTablesIfNotExists(ctx)
	if err != nil {
		panic(err)
	}
	defer dbmap.Cleanup(ctx)
	pu := &PersistentUser{43, "33r", false}
	err = dbmap.InsertContext(ctx, pu)
	if err != nil {
		panic(err)
	}

	// prove we can pass a pointer into Get
	pu2 := &PersistentUser{}
	err = dbmap.GetContext(ctx, pu2, pu.Key)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(pu, pu2) {
		t.Errorf("%v!=%v", pu, pu2)
	}

	arr := []*PersistentUser{}
	err = dbmap.SelectContext(ctx, &arr, "select * from persistentuser")
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(pu, arr[0]) {
		t.Errorf("%v!=%v", pu, arr[0])
	}

	// prove we can get the results back in a slice
	puArr := []PersistentUser{}
	err = dbmap.SelectContext(ctx, &puArr, "select * from persistentuser")
	if err != nil {
		t.Error(err)
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu, &puArr[0]) {
		t.Errorf("%v!=%v", pu, puArr[0])
	}
}

func TestOverrideVersionCol(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	dbmap.DropTables(ctx)

	t1 := dbmap.AddTable(InvoicePersonView{}).SetKeys(false, "invoiceid", "personid")
	err := dbmap.CreateTables(ctx)

	if err != nil {
		panic(err)
	}
	defer dbmap.Cleanup(ctx)
	c1 := t1.SetVersionCol("legacyversion")
	if c1.ColumnName != "legacyversion" {
		t.Errorf("Wrong col returned: %v", c1)
	}

	ipv := &InvoicePersonView{1, 2, "memo", "fname", 0}
	_update(ctx, dbmap, ipv)
	if ipv.LegacyVersion != 1 {
		t.Errorf("LegacyVersion not updated: %d", ipv.LegacyVersion)
	}
}

func TestDontPanicOnInsert(t *testing.T) {
	ctx := context.Background()
	var err error
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	err = dbmap.InsertContext(ctx, &TableWithNull{ID: 10})
	if err == nil {
		t.Errorf("Should have received an error for inserting without a known table.")
	}
}

func TestOptimisticLocking(t *testing.T) {
	ctx := context.Background()
	var err error
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	p1 := &Person{0, 0, 0, "Bob", "Smith", 0}
	dbmap.InsertContext(ctx, p1) // Version is now 1
	if p1.Version != 1 {
		t.Errorf("Insert didn't incr Version: %d != %d", 1, p1.Version)
		return
	}
	if p1.ID == 0 {
		t.Errorf("Insert didn't return a generated PK")
		return
	}

	p2 := &Person{}
	err = dbmap.GetContext(ctx, p2, p1.ID)
	if err != nil {
		panic(err)
	}
	p2.LName = "Edwards"
	_, err = dbmap.UpdateContext(ctx, p2) // Version is now 2

	if err != nil {
		panic(err)
	}

	if p2.Version != 2 {
		t.Errorf("Update didn't incr Version: %d != %d", 2, p2.Version)
	}

	p1.LName = "Howard"
	count, err := dbmap.UpdateContext(ctx, p1)
	if _, ok := err.(OptimisticLockError); !ok {
		t.Errorf("update - Expected OptimisticLockError, got: %v", err)
	}
	if count != -1 {
		t.Errorf("update - Expected -1 count, got: %d", count)
	}

	count, err = dbmap.DeleteContext(ctx, p1)
	if _, ok := err.(OptimisticLockError); !ok {
		t.Errorf("delete - Expected OptimisticLockError, got: %v", err)
	}
	if count != -1 {
		t.Errorf("delete - Expected -1 count, got: %d", count)
	}
}

// what happens if a legacy table has a null value?
func TestDoubleAddTable(t *testing.T) {
	dbmap := newDbMap()
	t1 := dbmap.AddTable(TableWithNull{}).SetKeys(false, "ID")
	t2 := dbmap.AddTable(TableWithNull{})
	if t1 != t2 {
		t.Errorf("%v != %v", t1, t2)
	}
}

// test overriding the create sql
func TestColMapCreateSql(t *testing.T) {
	ctx := context.Background()
	dbmap := newDbMap()
	t1 := dbmap.AddTable(TableWithNull{})
	b := t1.ColMap("Bytes")
	custom := "bytes text NOT NULL"
	b.SetSqlCreate(custom)
	var buf bytes.Buffer
	writeColumnSql(&buf, b)
	s := buf.String()
	if s != custom {
		t.Errorf("Expected custom sql `%s`, got %s", custom, s)
	}
	err := dbmap.CreateTables(ctx)
	defer dbmap.Cleanup(ctx)
	if err != nil {
		t.Error(err)
	}
}

// what happens if a legacy table has a null value?
func TestNullValues(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMapNulls(ctx)
	defer dbmap.Cleanup(ctx)

	// insert a row directly
	_, err := dbmap.ExecContext(ctx, `insert into tablewithnull values (10, null, null, null, null, null)`)
	if err != nil {
		panic(err)
	}

	// try to load it
	expected := &TableWithNull{ID: 10}
	t1 := &TableWithNull{}
	MustGet(ctx, dbmap, t1, 10)
	if !reflect.DeepEqual(expected, t1) {
		t.Errorf("%v != %v", expected, t1)
	}

	// update it
	t1.Str = sql.NullString{"hi", true}
	expected.Str = t1.Str
	t1.Int64 = sql.NullInt64{999, true}
	expected.Int64 = t1.Int64
	t1.Float64 = sql.NullFloat64{53.33, true}
	expected.Float64 = t1.Float64
	t1.Bool = sql.NullBool{true, true}
	expected.Bool = t1.Bool
	t1.Bytes = []byte{1, 30, 31, 33}
	expected.Bytes = t1.Bytes
	_update(ctx, dbmap, t1)

	MustGet(ctx, dbmap, t1, 10)
	if t1.Str.String != "hi" {
		t.Errorf("%s != hi", t1.Str.String)
	}
	if !reflect.DeepEqual(expected, t1) {
		t.Errorf("%v != %v", expected, t1)
	}
}

func TestColumnProps(t *testing.T) {
	ctx := context.Background()
	dbmap := newDbMap()
	//dbmap.TraceOn("", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	t1 := dbmap.AddTable(Invoice{}).SetKeys(true, "ID")
	//t1.ColMap("Created").Rename("date_created")
	t1.ColMap("Updated").SetTransient(true)
	t1.ColMap("Memo").SetMaxSize(10)
	t1.ColMap("PersonID").SetUnique(true)

	err := dbmap.CreateTables(ctx)
	if err != nil {
		panic(err)
	}
	defer dbmap.Cleanup(ctx)

	// test transient
	inv := &Invoice{0, 0, 1, "my invoice", 0, true}
	_insert(ctx, dbmap, inv)
	inv2 := Invoice{}
	MustGet(ctx, dbmap, &inv2, inv.ID)
	if inv2.Updated != 0 {
		t.Errorf("Saved transient column 'Updated'")
	}

	// test max size
	inv2.Memo = "this memo is too long"
	err = dbmap.InsertContext(ctx, inv2)
	if err == nil {
		t.Errorf("max size exceeded, but Insert did not fail.")
	}

	// test unique - same person id
	inv = &Invoice{0, 0, 1, "my invoice2", 0, false}
	err = dbmap.InsertContext(ctx, inv)
	if err == nil {
		t.Errorf("same PersonID inserted, but Insert did not fail.")
	}
}

func TestRawSelect(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	p1 := &Person{0, 0, 0, "bob", "smith", 0}
	_insert(ctx, dbmap, p1)

	inv1 := &Invoice{0, 0, 0, "xmas order", p1.ID, true}
	_insert(ctx, dbmap, inv1)

	expected := &InvoicePersonView{inv1.ID, p1.ID, inv1.Memo, p1.FName, 0}

	query := "select i.id invoiceid, p.id personid, i.memo, p.fname " +
		"from invoice_test i, person_test p " +
		"where i.personid = p.id"
	list := []InvoicePersonView{}
	MustSelect(ctx, dbmap, &list, query)
	if len(list) != 1 {
		t.Errorf("len(list) != 1: %d", len(list))
	} else if !reflect.DeepEqual(expected, &list[0]) {
		t.Errorf("%v != %v", expected, list[0])
	}
}

func TestHooks(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	p1 := &Person{0, 0, 0, "bob", "smith", 0}
	_insert(ctx, dbmap, p1)
	if p1.Created == 0 || p1.Updated == 0 {
		t.Errorf("p1.PreInsert() didn't run: %v", p1)
	} else if p1.LName != "postinsert" {
		t.Errorf("p1.PostInsert() didn't run: %v", p1)
	}

	MustGet(ctx, dbmap, p1, p1.ID)
	if p1.LName != "postget" {
		t.Errorf("p1.PostGet() didn't run: %v", p1)
	}

	p1.LName = "smith"
	_update(ctx, dbmap, p1)
	if p1.FName != "preupdate" {
		t.Errorf("p1.PreUpdate() didn't run: %v", p1)
	} else if p1.LName != "postupdate" {
		t.Errorf("p1.PostUpdate() didn't run: %v", p1)
	}

	var persons []*Person
	bindVar := dbmap.Dialect.BindVar(0)
	MustSelect(ctx, dbmap, &persons, "select * from person_test where id = "+bindVar, p1.ID)
	if persons[0].LName != "postget" {
		t.Errorf("p1.PostGet() didn't run after select: %v", p1)
	}

	_del(ctx, dbmap, p1)
	if p1.FName != "predelete" {
		t.Errorf("p1.PreDelete() didn't run: %v", p1)
	} else if p1.LName != "postdelete" {
		t.Errorf("p1.PostDelete() didn't run: %v", p1)
	}

	// Test error case
	p2 := &Person{0, 0, 0, "badname", "", 0}
	err := dbmap.InsertContext(ctx, p2)
	if err == nil {
		t.Errorf("p2.PreInsert() didn't return an error")
	}
}

func TestTransaction(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	inv1 := &Invoice{0, 100, 200, "t1", 0, true}
	inv2 := &Invoice{0, 100, 200, "t2", 0, false}

	trans, err := dbmap.BeginContext(ctx)
	if err != nil {
		panic(err)
	}
	trans.InsertContext(ctx, inv1, inv2)
	err = trans.Commit()
	if err != nil {
		panic(err)
	}

	obj := &Invoice{}
	err = dbmap.GetContext(ctx, obj, inv1.ID)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(inv1, obj) {
		t.Errorf("%v != %v", inv1, obj)
	}
	err = dbmap.GetContext(ctx, obj, inv2.ID)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(inv2, obj) {
		t.Errorf("%v != %v", inv2, obj)
	}
}

func TestMultiple(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	inv1 := &Invoice{0, 100, 200, "a", 0, false}
	inv2 := &Invoice{0, 100, 200, "b", 0, true}
	_insert(ctx, dbmap, inv1, inv2)

	inv1.Memo = "c"
	inv2.Memo = "d"
	_update(ctx, dbmap, inv1, inv2)

	count := _del(ctx, dbmap, inv1, inv2)
	if count != 2 {
		t.Errorf("%d != 2", count)
	}
}

func TestCrud(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	inv := &Invoice{0, 100, 200, "first order", 0, true}

	// INSERT row
	_insert(ctx, dbmap, inv)
	if inv.ID == 0 {
		t.Errorf("inv.ID was not set on INSERT")
		return
	}

	// SELECT row
	inv2 := &Invoice{}
	MustGet(ctx, dbmap, inv2, inv.ID)
	if !reflect.DeepEqual(inv, inv2) {
		t.Errorf("%v != %v", inv, inv2)
	}

	// UPDATE row and SELECT
	inv.Memo = "second order"
	inv.Created = 999
	inv.Updated = 11111
	count := _update(ctx, dbmap, inv)
	if count != 1 {
		t.Errorf("update 1 != %d", count)
	}

	MustGet(ctx, dbmap, inv2, inv.ID)
	if !reflect.DeepEqual(inv, inv2) {
		t.Errorf("%v != %v", inv, inv2)
	}

	// DELETE row
	deleted := _del(ctx, dbmap, inv)
	if deleted != 1 {
		t.Errorf("Did not delete row with ID: %d", inv.ID)
		return
	}

	// VERIFY deleted
	err := dbmap.GetContext(ctx, inv2, inv.ID)
	if err != sql.ErrNoRows {
		t.Errorf("Found invoice with id: %d after Delete()", inv.ID)
	}
}

func TestWithIgnoredColumn(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	ic := &WithIgnoredColumn{-1, 0, 1}
	_insert(ctx, dbmap, ic)
	expected := &WithIgnoredColumn{0, 1, 1}

	ic2 := &WithIgnoredColumn{}
	MustGet(ctx, dbmap, ic2, ic.ID)

	if !reflect.DeepEqual(expected, ic2) {
		t.Errorf("%v != %v", expected, ic2)
	}

	if _del(ctx, dbmap, ic) != 1 {
		t.Errorf("Did not delete row with ID: %d", ic.ID)
		return
	}

	err := dbmap.GetContext(ctx, ic2, ic.ID)
	if err != sql.ErrNoRows {
		t.Errorf("Found id: %d after Delete() (%#v)", ic.ID, ic2)
	}
}

func TestVersionMultipleRows(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	persons := []*Person{
		&Person{0, 0, 0, "Bob", "Smith", 0},
		&Person{0, 0, 0, "Jane", "Smith", 0},
		&Person{0, 0, 0, "Mike", "Smith", 0},
	}

	_insert(ctx, dbmap, persons[0], persons[1], persons[2])

	for x, p := range persons {
		if p.Version != 1 {
			t.Errorf("person[%d].Version != 1: %d", x, p.Version)
		}
	}
}

func TestWithStringPk(t *testing.T) {
	ctx := context.Background()
	dbmap := newDbMap()
	//dbmap.TraceOn("", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	dbmap.AddTableWithName(WithStringPk{}, "string_pk_test").SetKeys(true, "ID")
	_, err := dbmap.ExecContext(ctx, "create table string_pk_test (ID varchar(255), Name varchar(255));")
	if err != nil {
		t.Errorf("couldn't create string_pk_test: %v", err)
	}
	defer dbmap.Cleanup(ctx)

	row := &WithStringPk{"1", "foo"}
	err = dbmap.InsertContext(ctx, row)
	if err == nil {
		t.Errorf("Expected error when inserting into table w/non Int PK and autoincr set true")
	}
}

func BenchmarkNativeCrud(b *testing.B) {
	ctx := context.Background()
	var err error

	b.StopTimer()
	dbmap := initDbMapBench(ctx)
	defer dbmap.Cleanup(ctx)
	b.StartTimer()

	insert := "insert into invoice_test (date_created, updated, memo, personid) values (?, ?, ?, ?)"
	sel := "select id, date_created, updated, memo, personid from invoice_test where id=?"
	update := "update invoice_test set date_created=?, updated=?, memo=?, personid=? where id=?"
	delete := "delete from invoice_test where id=?"

	suffix := dbmap.Dialect.AutoIncrInsertSuffix(&ColumnMap{ColumnName: "id"})

	insert = ReBind(insert, dbmap.Dialect) + suffix
	sel = ReBind(sel, dbmap.Dialect)
	update = ReBind(update, dbmap.Dialect)
	delete = ReBind(delete, dbmap.Dialect)

	inv := &Invoice{0, 100, 200, "my memo", 0, false}

	for i := 0; i < b.N; i++ {
		if len(suffix) == 0 {
			res, err := dbmap.Db.Exec(insert, inv.Created, inv.Updated, inv.Memo, inv.PersonID)
			if err != nil {
				panic(err)
			}

			newid, err := res.LastInsertId()
			if err != nil {
				panic(err)
			}
			inv.ID = newid
		} else {
			rows, err := dbmap.Db.Query(insert, inv.Created, inv.Updated, inv.Memo, inv.PersonID)
			if err != nil {
				panic(err)
			}

			if rows.Next() {
				err = rows.Scan(&inv.ID)
				if err != nil {
					panic(err)
				}
			}
			rows.Close()

		}

		row := dbmap.Db.QueryRow(sel, inv.ID)
		err = row.Scan(&inv.ID, &inv.Created, &inv.Updated, &inv.Memo, &inv.PersonID)
		if err != nil {
			panic(err)
		}

		inv.Created = 1000
		inv.Updated = 2000
		inv.Memo = "my memo 2"
		inv.PersonID = 3000

		_, err = dbmap.Db.Exec(update, inv.Created, inv.Updated, inv.Memo,
			inv.PersonID, inv.ID)
		if err != nil {
			panic(err)
		}

		_, err = dbmap.Db.Exec(delete, inv.ID)
		if err != nil {
			panic(err)
		}
	}

}

func BenchmarkModlCrud(b *testing.B) {
	ctx := context.Background()
	b.StopTimer()
	dbmap := initDbMapBench(ctx)
	defer dbmap.Cleanup(ctx)
	//dbmap.TraceOn("", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	b.StartTimer()

	inv := &Invoice{0, 100, 200, "my memo", 0, true}
	for i := 0; i < b.N; i++ {
		err := dbmap.InsertContext(ctx, inv)
		if err != nil {
			panic(err)
		}

		inv2 := Invoice{}
		err = dbmap.GetContext(ctx, &inv2, inv.ID)
		if err != nil {
			panic(err)
		}

		inv2.Created = 1000
		inv2.Updated = 2000
		inv2.Memo = "my memo 2"
		inv2.PersonID = 3000
		_, err = dbmap.UpdateContext(ctx, &inv2)
		if err != nil {
			panic(err)
		}

		_, err = dbmap.DeleteContext(ctx, &inv2)
		if err != nil {
			panic(err)
		}

	}
}

func initDbMapBench(ctx context.Context) *DbMap {
	dbmap := newDbMap()
	dbmap.Db.Exec("drop table if exists invoice_test")
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "id")
	err := dbmap.CreateTables(ctx)
	if err != nil {
		panic(err)
	}
	return dbmap
}

func (d *DbMap) Cleanup(ctx context.Context) {
	err := d.DropTables(ctx)
	if err != nil {
		panic(err)
	}
	err = d.Dbx.Close()
	if err != nil {
		panic(err)
	}
}

func initDbMap(ctx context.Context) *DbMap {
	dbmap := newDbMap()
	//dbmap.TraceOn("", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "id")
	dbmap.AddTableWithName(Person{}, "person_test").SetKeys(true, "id")
	dbmap.AddTableWithName(WithIgnoredColumn{}, "ignored_column_test").SetKeys(true, "id")
	dbmap.AddTableWithName(WithTime{}, "time_test").SetKeys(true, "ID")
	err := dbmap.CreateTables(ctx)
	if err != nil {
		panic(err)
	}

	return dbmap
}

func TestTruncateTables(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)
	err := dbmap.CreateTablesIfNotExists(ctx)
	if err != nil {
		t.Error(err)
	}

	// Insert some data
	p1 := &Person{0, 0, 0, "Bob", "Smith", 0}
	dbmap.InsertContext(ctx, p1)
	inv := &Invoice{0, 0, 1, "my invoice", 0, true}
	dbmap.InsertContext(ctx, inv)

	err = dbmap.TruncateTables(ctx)
	if err != nil {
		t.Error(err)
	}

	// Make sure all rows are deleted
	people := []Person{}
	invoices := []Invoice{}
	dbmap.SelectContext(ctx, &people, "SELECT * FROM person_test")
	if len(people) != 0 {
		t.Errorf("Expected 0 person rows, got %d", len(people))
	}
	dbmap.SelectContext(ctx, &invoices, "SELECT * FROM invoice_test")
	if len(invoices) != 0 {
		t.Errorf("Expected 0 invoice rows, got %d", len(invoices))
	}
}

func TestTruncateTablesIdentityRestart(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)
	err := dbmap.CreateTablesIfNotExists(ctx)
	if err != nil {
		t.Error(err)
	}

	// Insert some data
	p1 := &Person{0, 0, 0, "Bob", "Smith", 0}
	dbmap.InsertContext(ctx, p1)
	inv := &Invoice{0, 0, 1, "my invoice", 0, true}
	dbmap.InsertContext(ctx, inv)

	err = dbmap.TruncateTablesIdentityRestart(ctx)
	if err != nil {
		t.Error(err)
	}

	// Make sure all rows are deleted
	people := []Person{}
	invoices := []Invoice{}
	dbmap.SelectContext(ctx, &people, "SELECT * FROM person_test")
	if len(people) != 0 {
		t.Errorf("Expected 0 person rows, got %d", len(people))
	}
	dbmap.SelectContext(ctx, &invoices, "SELECT * FROM invoice_test")
	if len(invoices) != 0 {
		t.Errorf("Expected 0 invoice rows, got %d", len(invoices))
	}

	p2 := &Person{0, 0, 0, "Other", "Person", 0}
	dbmap.InsertContext(ctx, p2)
	if p2.ID != int64(1) {
		t.Errorf("Expected new person ID to be equal to 1, was %d", p2.ID)
	}
}

func TestSelectBehavior(t *testing.T) {
	ctx := context.Background()
	db := initDbMap(ctx)
	defer db.Cleanup(ctx)

	p := Person{}

	// check that SelectOne with no rows returns ErrNoRows
	err := db.SelectOneContext(ctx, &p, "select * from person_test")
	if err == nil || err != sql.ErrNoRows {
		t.Fatal(err)
	}

	// insert and ensure SelectOne works properly
	bob := Person{0, 0, 0, "Bob", "Smith", 0}
	db.InsertContext(ctx, &bob)

	err = db.SelectOneContext(ctx, &p, "select * from person_test")
	if err != nil {
		t.Fatal(err)
	}
	if p.FName != "Bob" {
		t.Errorf("Wrong FName: %s", p.FName)
	}
	// there's a post hook on this that sets it to postget, ensure it ran
	if p.LName != "postget" {
		t.Errorf("Wrong LName: %s", p.LName)
	}

	// insert again and ensure SelectOne *does not* error in rows > 1
	ben := Person{0, 0, 0, "Ben", "Smith", 0}
	db.InsertContext(ctx, &ben)

	err = db.SelectOneContext(ctx, &p, "select * from person_test ORDER BY fname ASC")
	if err != nil {
		t.Fatal(err)
	}
	if p.FName != "Ben" {
		t.Errorf("Wrong FName: %s", p.FName)
	}
}

func TestQuoteTableNames(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	quotedTableName := dbmap.Dialect.QuoteField("person_test")

	// Use a buffer to hold the log to check generated queries
	var logBuffer bytes.Buffer
	dbmap.TraceOn("", log.New(&logBuffer, "modltest:", log.Lmicroseconds))

	// Create some rows
	p1 := &Person{0, 0, 0, "bob", "smith", 0}
	errorTemplate := "Expected quoted table name %v in query but didn't find it"

	// Check if Insert quotes the table name
	id := dbmap.InsertContext(ctx, p1)
	if !bytes.Contains(logBuffer.Bytes(), []byte(quotedTableName)) {
		t.Log("log:", logBuffer.String())
		t.Errorf(errorTemplate, quotedTableName)
	}
	logBuffer.Reset()

	// Check if Get quotes the table name
	dbmap.GetContext(ctx, Person{}, id)
	if !bytes.Contains(logBuffer.Bytes(), []byte(quotedTableName)) {
		t.Errorf(errorTemplate, quotedTableName)
	}
	logBuffer.Reset()
}

type WithTime struct {
	ID   int64
	Time time.Time
}

func TestWithTime(t *testing.T) {
	ctx := context.Background()
	dbmap := initDbMap(ctx)
	defer dbmap.Cleanup(ctx)

	// FIXME: there seems to be a bug with go-sql-driver and timezones?
	// MySQL doesn't have any timestamp support, but since it is not
	// sending any, the scan assumes UTC, so the scanner should
	// probably convert to UTC before storing.  Also, note that time.Time
	// support requires a special bit to be added to the DSN
	t1, err := time.Parse("2006-01-02 15:04:05 -0700 MST",
		"2013-08-09 21:30:43 +0000 UTC")
	if err != nil {
		t.Fatal(err)
	}

	w1 := WithTime{1, t1}
	dbmap.InsertContext(ctx, &w1)

	w2 := WithTime{}
	dbmap.GetContext(ctx, &w2, w1.ID)

	if w1.Time.UnixNano() != w2.Time.UnixNano() {
		t.Errorf("%v != %v", w1, w2)
	}
}

func initDbMapNulls(ctx context.Context) *DbMap {
	dbmap := newDbMap()
	//dbmap.TraceOn("", log.New(os.Stdout, "modltest: ", log.Lmicroseconds))
	dbmap.AddTable(TableWithNull{}).SetKeys(false, "id")
	err := dbmap.CreateTables(ctx)
	if err != nil {
		panic(err)
	}
	return dbmap
}

func newDbMap() *DbMap {
	dialect, driver := dialectAndDriver()
	return NewDbMap(connect(driver), dialect)
}

func connect(driver string) *sql.DB {
	dsn := os.Getenv("MODL_TEST_DSN")
	if dsn == "" {
		panic("MODL_TEST_DSN env variable is not set. Please see README.md")
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		panic("Error connecting to db: " + err.Error())
	}
	err = db.Ping()
	if err != nil {
		panic("Error connecting to db: " + err.Error())
	}
	return db
}

func dialectAndDriver() (Dialect, string) {
	switch os.Getenv("MODL_TEST_DIALECT") {
	case "mysql":
		return MySQLDialect{"InnoDB", "UTF8"}, "mysql"
	case "postgres":
		return PostgresDialect{}, "postgres"
	case "sqlite":
		return SqliteDialect{}, "sqlite3"
	}
	panic("MODL_TEST_DIALECT env variable is not set or is invalid. Please see README.md")
}

func _insert(ctx context.Context, dbmap *DbMap, list ...interface{}) {
	err := dbmap.InsertContext(ctx, list...)
	if err != nil {
		panic(err)
	}
}

func _update(ctx context.Context, dbmap *DbMap, list ...interface{}) int64 {
	count, err := dbmap.UpdateContext(ctx, list...)
	if err != nil {
		panic(err)
	}
	return count
}

func _del(ctx context.Context, dbmap *DbMap, list ...interface{}) int64 {
	count, err := dbmap.DeleteContext(ctx, list...)
	if err != nil {
		panic(err)
	}

	return count
}

func MustGet(ctx context.Context, dbmap *DbMap, i interface{}, keys ...interface{}) {
	err := dbmap.GetContext(ctx, i, keys...)
	if err != nil {
		panic(err)
	}
}

func MustSelect(ctx context.Context, dbmap *DbMap, dest interface{}, query string, args ...interface{}) {
	err := dbmap.SelectContext(ctx, dest, query, args...)
	if err != nil {
		panic(err)
	}
}
