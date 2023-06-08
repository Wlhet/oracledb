package oracledb

import (
	"context"
	"database/sql"
	"errors"
)

func (oc *OracleClient) Query(query string, args ...any) (rows *sql.Rows, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	return oc.db.Query(query, args...)
}

func (oc *OracleClient) QueryRow(query string, args ...any) (row *sql.Row, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	row = oc.db.QueryRow(query, args...)
	return
}

func (oc *OracleClient) QueryContext(ctx context.Context, query string, args ...any) (row *sql.Rows, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	return oc.db.QueryContext(ctx, query, args...)
}

func (oc *OracleClient) QueryRowContext(ctx context.Context, query string, args ...any) (row *sql.Row, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	row = oc.db.QueryRowContext(ctx, query, args...)
	return
}
