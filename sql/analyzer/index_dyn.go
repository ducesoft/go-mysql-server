// Copyright 2022 Dolthub, Inc.
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

package analyzer

import "github.com/dolthub/go-mysql-server/sql"

const DynIndex = "DYN"

type DynamicIndex interface {
	// ExpressionDyn returns the indexed expressions. If the result is more than
	// one expression, it means the index has multiple columns indexed. If it's
	// just one, it means it may be an expression or a column.
	ExpressionDyn(expressions ...sql.Expression) []string
}

func Expressions(idx sql.Index, expressions ...sql.Expression) []string {
	if idx.IndexType() != DynIndex {
		return idx.Expressions()
	}
	if i, ok := idx.(DynamicIndex); ok {
		return i.ExpressionDyn(expressions...)
	}
	return idx.Expressions()
}
