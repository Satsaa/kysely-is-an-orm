import {
	Kysely,
	InsertQueryBuilder,
	InsertResult,
	UpdateQueryBuilder,
	UpdateResult,
	DeleteQueryBuilder,
	DeleteResult,
	type Selectable,
	type ReferenceExpression,
	type ComparisonOperatorExpression,
	type OperandValueExpressionOrList,
	type ExpressionOrFactory,
	type SqlBool,
	type Insertable,
	type Updateable,
	type UpdateObject,
} from "kysely";
import { type MetaDB } from "./meta.js";
import { OrmReturningBuilder } from "./returning-builder.js";

type SelectableColumn<DB extends Record<string, any>, TB extends keyof DB & string> =
	Extract<keyof Selectable<DB[TB]>, string>;

type ReturningColumn<DB extends Record<string, any>, TB extends keyof DB & string> =
	| SelectableColumn<DB, TB>
	| `${TB}.${SelectableColumn<DB, TB>}`;

type UnqualifiedColumn<TB extends string, C extends string> =
	C extends `${TB}.${infer Column}` ? Column : C;

type ReturningColumnKey<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	C extends string,
> = Extract<UnqualifiedColumn<TB, C>, keyof Selectable<DB[TB]>>;

type ReturningOutput<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	C extends string,
> = Pick<Selectable<DB[TB]>, ReturningColumnKey<DB, TB, C>>;

// ---------------------------------------------------------------------------
// OrmInsertQueryBuilder
// ---------------------------------------------------------------------------

export class OrmInsertQueryBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
> {
	constructor(
		private readonly _db: Kysely<DB>,
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _inner: InsertQueryBuilder<DB, TB, InsertResult>,
	) {}

	values(values: Insertable<DB[TB]> | ReadonlyArray<Insertable<DB[TB]>>): OrmInsertQueryBuilder<DB, TB, M> {
		return new OrmInsertQueryBuilder(this._db, this._meta, this._table, this._inner.values(values));
	}

	onConflict(handler: (oc: any) => any): OrmInsertQueryBuilder<DB, TB, M> {
		return new OrmInsertQueryBuilder(this._db, this._meta, this._table, this._inner.onConflict(handler));
	}

	returning<C extends ReturningColumn<DB, TB>>(
		column: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C>, M>;
	returning<const C extends readonly ReturningColumn<DB, TB>[]>(
		columns: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C[number]>, M>;
	returning(selection: ReturningColumn<DB, TB> | readonly ReturningColumn<DB, TB>[]): OrmReturningBuilder<DB, TB, any, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], [], selection);
	}

	returningAll(): OrmReturningBuilder<DB, TB, Selectable<DB[TB]>, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], []);
	}

	async execute() { return this._inner.execute(); }
	compile() { return this._inner.compile(); }
}

// ---------------------------------------------------------------------------
// OrmUpdateQueryBuilder
// ---------------------------------------------------------------------------

export class OrmUpdateQueryBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
> {
	constructor(
		private readonly _db: Kysely<DB>,
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _inner: UpdateQueryBuilder<DB, TB, TB, UpdateResult>,
		private readonly _wheres: any[][] = [],
	) {}

	set(values: UpdateObject<DB, TB, TB>): OrmUpdateQueryBuilder<DB, TB, M> {
		return new OrmUpdateQueryBuilder(this._db, this._meta, this._table, this._inner.set(values), this._wheres);
	}

	where<RE extends ReferenceExpression<DB, TB>>(
		lhs: RE, op: ComparisonOperatorExpression, rhs: OperandValueExpressionOrList<DB, TB, RE>,
	): OrmUpdateQueryBuilder<DB, TB, M>;
	where<E extends ExpressionOrFactory<DB, TB, SqlBool>>(expression: E): OrmUpdateQueryBuilder<DB, TB, M>;
	where(...args: any[]): OrmUpdateQueryBuilder<DB, TB, M> {
		return new OrmUpdateQueryBuilder(this._db, this._meta, this._table, (this._inner as any).where(...args), [...this._wheres, args]);
	}

	returning<C extends ReturningColumn<DB, TB>>(
		column: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C>, M>;
	returning<const C extends readonly ReturningColumn<DB, TB>[]>(
		columns: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C[number]>, M>;
	returning(selection: ReturningColumn<DB, TB> | readonly ReturningColumn<DB, TB>[]): OrmReturningBuilder<DB, TB, any, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], this._wheres, selection);
	}

	returningAll(): OrmReturningBuilder<DB, TB, Selectable<DB[TB]>, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], this._wheres);
	}

	async execute() { return this._inner.execute(); }
	compile() { return this._inner.compile(); }
}

// ---------------------------------------------------------------------------
// OrmDeleteQueryBuilder
// ---------------------------------------------------------------------------

export class OrmDeleteQueryBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
> {
	constructor(
		private readonly _db: Kysely<DB>,
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _inner: DeleteQueryBuilder<DB, TB, DeleteResult>,
		private readonly _wheres: any[][] = [],
	) {}

	where<RE extends ReferenceExpression<DB, TB>>(
		lhs: RE, op: ComparisonOperatorExpression, rhs: OperandValueExpressionOrList<DB, TB, RE>,
	): OrmDeleteQueryBuilder<DB, TB, M>;
	where<E extends ExpressionOrFactory<DB, TB, SqlBool>>(expression: E): OrmDeleteQueryBuilder<DB, TB, M>;
	where(...args: any[]): OrmDeleteQueryBuilder<DB, TB, M> {
		return new OrmDeleteQueryBuilder(this._db, this._meta, this._table, (this._inner as any).where(...args), [...this._wheres, args]);
	}

	returning<C extends ReturningColumn<DB, TB>>(
		column: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C>, M>;
	returning<const C extends readonly ReturningColumn<DB, TB>[]>(
		columns: C,
	): OrmReturningBuilder<DB, TB, ReturningOutput<DB, TB, C[number]>, M>;
	returning(selection: ReturningColumn<DB, TB> | readonly ReturningColumn<DB, TB>[]): OrmReturningBuilder<DB, TB, any, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], this._wheres, selection);
	}

	returningAll(): OrmReturningBuilder<DB, TB, Selectable<DB[TB]>, M> {
		return new OrmReturningBuilder(this._db, this._meta, this._table, this._inner, [], this._wheres);
	}

	async execute() { return this._inner.execute(); }
	compile() { return this._inner.compile(); }
}
