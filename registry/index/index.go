package index

import (
	"database/sql"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/notifications"
	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultLimit = 20
)

type Record struct {
	Repository string    `json:"repository"`
	Digest     string    `json:"digest"`
	Url        string    `json:"url"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type QueryArgs struct {
	Keyword string
	Skip    int
	Limit   int
}

func (self *QueryArgs) prepare() {
	if self.Skip < 0 {
		self.Skip = 0
	}
	if self.Limit < 1 {
		self.Limit = defaultLimit
	}
}

type IndexService struct {
	db *sql.DB
}

func New(configuration *configuration.Configuration) (*IndexService, error) {
	var (
		err   error
		srv   = &IndexService{}
		stmts [2]string
	)
	srv.db, err = sql.Open("sqlite3", "/registry.sqlite3")
	if err != nil {
		logrus.Error("Failed to open database: ", err)
		return nil, err
	}

	stmts[0] = `create table if not exists repositories (
		id         integer primary key,
		repository varchar(256),
		digest     varchar(80),
		url        varchar(256),
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`
	stmts[1] = `create unique index if not exists idx_name on repositories(repository)`
	for _, stmt := range stmts {
		if _, err := srv.db.Exec(stmt); err != nil {
			logrus.Error("Failed to prepare database: ", err)
			return nil, err
		}
	}

	return srv, nil
}

func (self *IndexService) Write(events ...notifications.Event) error {
	for _, event := range events {
		if event.Target.MediaType == manifest.ManifestMediaType {
			if event.Action == notifications.EventActionDelete {
				if err := self.delete(event); err != nil {
					return err
				}
			} else {
				if err := self.add(event); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (self *IndexService) delete(event notifications.Event) error {
	query := "delete from repositories where repository=?"
	stmt, err := self.db.Prepare(query)
	if err != nil {
		logrus.Error("sqlite prepare: ", err)
		return err
	}
	_, err = stmt.Exec(event.Target.Repository)
	return err
}

func (self *IndexService) add(event notifications.Event) error {
	query := "replace into repositories(repository, digest, url, updated_at) values(?,?,?,?)"
	stmt, err := self.db.Prepare(query)
	if err != nil {
		logrus.Error("sqlite prepare: ", err)
		return err
	}
	defer stmt.Close()

	target := event.Target
	if _, err := stmt.Exec(target.Repository, string(target.Digest), target.URL, time.Now()); err != nil {
		logrus.Error("sqlite insert: ", err)
		return err
	}
	return nil
}

func (self *IndexService) Close() error {
	logrus.Debug("index service close")
	self.db.Close()
	return nil
}

func (self *IndexService) Sink() notifications.Sink {
	return self
}

func (self *IndexService) GetPage(args QueryArgs) ([]Record, error) {
	args.prepare()
	query := "select repository, digest, url, updated_at from repositories "
	if len(args.Keyword) > 0 {
		query += " where repository like ? "
	}
	query += " limit ? offset ?"

	stmt, err := self.db.Prepare(query)
	if err != nil {
		logrus.Error("select prepare: ", err)
		return nil, err
	}

	var rows *sql.Rows

	if len(args.Keyword) > 0 {
		rows, err = stmt.Query("%"+args.Keyword+"%", args.Limit, args.Skip)
	} else {
		rows, err = stmt.Query(args.Limit, args.Skip)
	}

	if err != nil {
		logrus.Error("sqlite query: ", err)
		return nil, err
	}

	records := []Record{}
	for rows.Next() {
		record := Record{}
		err = rows.Scan(&record.Repository, &record.Digest, &record.Url, &record.UpdatedAt)
		if err != nil {
			logrus.Error("failed to scan rows: ", err)
			continue
		}
		records = append(records, record)
	}
	return records, nil
}
