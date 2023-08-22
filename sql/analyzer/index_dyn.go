/*
 * Copyright (c) 2000, 2099, ducesoft and/or its affiliates. All rights reserved.
 * DUCESOFT PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 */

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
