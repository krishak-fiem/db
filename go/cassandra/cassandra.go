package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/krishak-fiem/utils/go/env"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/table"
	"log"
)

var Session gocqlx.Session

func init() {
	cluster := gocql.NewCluster(env.Get("CASSANDRA_HOST"))
	cluster.Port = 9042
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal(err)
	}
	session.KeyspaceMetadata("public")
	err = session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS public
									WITH REPLICATION = { 
									'class' : 'NetworkTopologyStrategy',
									'replication_factor' : 1
							};`)
	if err != nil {
		log.Fatal(err)
	}
	Session = session
}

func CreateMetadata(tableName string, columns []string, partKey []string, sortKey []string) table.Metadata {
	return table.Metadata{
		Name:    tableName,
		Columns: columns,
		PartKey: partKey,
		SortKey: sortKey,
	}
}

func CreateTable(metaData table.Metadata) *table.Table {
	return table.New(metaData)
}

func InsertRow(table table.Table, record interface{}) error {
	q := Session.Query(table.Insert()).BindStruct(record)
	if err := q.ExecRelease(); err != nil {
		return err
	}
	return nil
}

func GetRow(table table.Table, record interface{}) error {
	q := Session.Query(table.Get()).BindStruct(record)
	if err := q.GetRelease(&record); err != nil {
		return err
	}
	return nil
}

func GetRows(table table.Table, query map[string]interface{}, result []interface{}) error {
	q := Session.Query(table.Select()).BindStruct(query)
	if err := q.SelectRelease(&result); err != nil {
		return err
	}
	return nil
}

func UpdateRow(table table.Table, query map[string]interface{}, record interface{}) error {
	q := Session.Query(table.Update()).BindMap(query)
	if err := q.ExecRelease(); err != nil {
		return err
	}
	return nil
}

func DeleteRow(table table.Table, query map[string]interface{}, record interface{}) error {
	q := Session.Query(table.Delete()).BindMap(query)
	if err := q.ExecRelease(); err != nil {
		return err
	}
	return nil
}
