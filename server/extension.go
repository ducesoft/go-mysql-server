// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"sort"
)

func Intercept(h Interceptor) {
	inters = append(inters, h)
	sort.Slice(inters, func(i, j int) bool { return inters[i].Priority() < inters[j].Priority() })
}

var inters []Interceptor

func withChain(h mysql.Handler) mysql.Handler {
	var last Chain = h
	for i := len(inters) - 1; i >= 0; i-- {
		filter := inters[i]
		next := last
		last = &chainInterceptor{i: filter, c: next}
	}
	return &interceptorHandler{h: h, c: last}
}

type Interceptor interface {

	// Priority returns the priority of the interceptor.
	Priority() int

	// Query is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	Query(chain Chain, c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) error

	// MultiQuery is called when a connection receives a query and the
	// client supports MULTI_STATEMENT. It should process the first
	// statement in |query| and return the remainder. It will be called
	// multiple times until the remainder is |""|.
	MultiQuery(chain Chain, c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) (string, error)

	// Prepare is called when a connection receives a prepared
	// statement query.
	Prepare(chain Chain, c *mysql.Conn, query string) ([]*querypb.Field, error)

	// StmtExecute is called when a connection receives a statement
	// execute query.
	StmtExecute(chain Chain, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error
}

type Chain interface {

	// ComQuery is called when a connection receives a query.
	// Note the contents of the query slice may change after
	// the first call to callback. So the Handler should not
	// hang on to the byte slice.
	ComQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) error

	// ComMultiQuery is called when a connection receives a query and the
	// client supports MULTI_STATEMENT. It should process the first
	// statement in |query| and return the remainder. It will be called
	// multiple times until the remainder is |""|.
	ComMultiQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) (string, error)

	// ComPrepare is called when a connection receives a prepared
	// statement query.
	ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error)

	// ComStmtExecute is called when a connection receives a statement
	// execute query.
	ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error
}

type chainInterceptor struct {
	i Interceptor
	c Chain
}

func (that *chainInterceptor) ComQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) error {
	return that.i.Query(that.c, c, query, callback)
}

func (that *chainInterceptor) ComMultiQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) (string, error) {
	return that.i.MultiQuery(that.c, c, query, callback)
}

func (that *chainInterceptor) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return that.i.Prepare(that.c, c, query)
}

func (that *chainInterceptor) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return that.i.StmtExecute(that.c, c, prepare, callback)
}

type interceptorHandler struct {
	c Chain
	h mysql.Handler
}

func (that *interceptorHandler) NewConnection(c *mysql.Conn) {
	that.h.NewConnection(c)
}

func (that *interceptorHandler) ConnectionClosed(c *mysql.Conn) {
	that.h.ConnectionClosed(c)
}

func (that *interceptorHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	return that.h.ComInitDB(c, schemaName)
}

func (that *interceptorHandler) ComQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) error {
	return that.c.ComQuery(c, query, callback)
}

func (that *interceptorHandler) ComMultiQuery(c *mysql.Conn, query string, callback func(res *sqltypes.Result, more bool) error) (string, error) {
	return that.c.ComMultiQuery(c, query, callback)
}

func (that *interceptorHandler) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, error) {
	return that.c.ComPrepare(c, query)
}

func (that *interceptorHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return that.c.ComStmtExecute(c, prepare, callback)
}

func (that *interceptorHandler) WarningCount(c *mysql.Conn) uint16 {
	return that.h.WarningCount(c)
}

func (that *interceptorHandler) ComResetConnection(c *mysql.Conn) {
	that.h.ComResetConnection(c)
}

func (that *interceptorHandler) ParserOptionsForConnection(c *mysql.Conn) (ast.ParserOptions, error) {
	return that.h.ParserOptionsForConnection(c)
}
