package oracledb

import (
	"context"
	"database/sql"
	"errors"
)

func (oc *OracleClient) Exec(query string, args ...any) (result sql.Result, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	result, err = oc.db.Exec(query, args...)
	return result, err
}

func (oc *OracleClient) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	if oc.conf.recoverPanic {
		defer func() {
			if panicError := recover(); panicError != nil {
				err = errors.New(panicError.(string))
			}
		}()
	}
	return oc.db.ExecContext(ctx, query, args...)
}
