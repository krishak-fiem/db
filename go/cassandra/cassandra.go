package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/krishak-fiem/utils/go/env"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
	"log"
)

var Session gocqlx.Session

func Init(port int) gocqlx.Session {
	cluster := gocql.NewCluster(env.Get("CASSANDRA_HOST"))
	cluster.Port = port
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
	return session
}

func CreateMetadata(tableName string, columns []string, partKey []string) table.Metadata {
	return table.Metadata{
		Name:    tableName,
		Columns: columns,
		PartKey: partKey,
	}
}

func CreateTable(metaData table.Metadata) *table.Table {
	return table.New(metaData)
}

func InsertRow(session gocqlx.Session, table table.Table, record interface{}) error {
	q := session.Query(table.Insert()).BindStruct(record)
	if err := q.ExecRelease(); err != nil {
		return err
	}
	return nil
}

func GetRow(session gocqlx.Session, table table.Table, record interface{}) error {
	q := session.Query(table.Get()).BindStruct(record)
	if err := q.GetRelease(record); err != nil {
		fmt.Println(err.Error())
		return err
	}

	return nil
}

func GetRows(session gocqlx.Session, table table.Table, query map[string]interface{}, result []interface{}) error {
	q := session.Query(table.Select()).BindMap(query)
	if err := q.SelectRelease(&result); err != nil {
		return err
	}
	return nil
}

func UpdateRow(session gocqlx.Session, table table.Table, columns []string, where string, record interface{}) error {
	q := qb.Update(table.Name()).Set(columns...).Where(qb.Eq(where)).Query(session).SerialConsistency(gocql.Serial).BindStruct(record)
	_, err := q.ExecCASRelease()
	if err != nil {
		return err
	}
	return nil
}

func DeleteRow(session gocqlx.Session, table table.Table, query map[string]interface{}, record interface{}) error {
	q := session.Query(table.Delete()).BindMap(query)
	if err := q.ExecRelease(); err != nil {
		return err
	}
	return nil
}
