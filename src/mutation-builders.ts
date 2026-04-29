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
	type ValueExpression,
	type ColumnType,
	type OnConflictBuilder,
	type OnConflictDatabase,
	type OnConflictDoNothingBuilder,
	type OnConflictTables,
	type OnConflictUpdateBuilder,
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

type DeepInsertType<T> = T extends ColumnType<any, infer I, any>
	? undefined extends I
		? DeepInsertType<Exclude<I, undefined>> | undefined
		: DeepInsertType<I>
	: T;

type DeepUpdateType<T> = T extends ColumnType<any, any, infer U> ? DeepUpdateType<U> : T;

type InsertValue<DB, TB extends keyof DB, T> = ValueExpression<DB, TB, DeepInsertType<T>>;
type UpdateValue<DB, TB extends keyof DB, T> = ValueExpression<DB, TB, DeepUpdateType<T>>;

type OptionalInsertKeys<R> = {
	[K in keyof R]: undefined extends DeepInsertType<R[K]>
		? K
		: null extends DeepInsertType<R[K]>
			? K
			: never;
}[keyof R];

type RequiredInsertKeys<R> = Exclude<keyof R, OptionalInsertKeys<R>>;

type DeepInsertable<DB extends Record<string, any>, TB extends keyof DB & string> = {
	[K in RequiredInsertKeys<DB[TB]>]: InsertValue<DB, TB, DB[TB][K]>;
} & {
	[K in OptionalInsertKeys<DB[TB]>]?: InsertValue<DB, TB, DB[TB][K]>;
};

type OrmInsertable<DB extends Record<string, any>, TB extends keyof DB & string> =
	| Insertable<DB[TB]>
	| DeepInsertable<DB, TB>;

type DeepUpdateObject<DB extends Record<string, any>, TB extends keyof DB & string> = {
	[K in keyof DB[TB]]?: UpdateValue<DB, TB, DB[TB][K]>;
};

type OrmUpdateObject<DB extends Record<string, any>, TB extends keyof DB & string> =
	| Updateable<DB[TB]>
	| DeepUpdateObject<DB, TB>;

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

	values(values: OrmInsertable<DB, TB> | ReadonlyArray<OrmInsertable<DB, TB>>): OrmInsertQueryBuilder<DB, TB, M> {
		return new OrmInsertQueryBuilder(
			this._db,
			this._meta,
			this._table,
			this._inner.values(values as Parameters<InsertQueryBuilder<DB, TB, InsertResult>["values"]>[0]),
		);
	}

	onConflict(
		handler: (oc: OnConflictBuilder<DB, TB>) =>
			| OnConflictUpdateBuilder<OnConflictDatabase<DB, TB>, OnConflictTables<TB>>
			| OnConflictDoNothingBuilder<DB, TB>,
	): OrmInsertQueryBuilder<DB, TB, M> {
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

	set(values: OrmUpdateObject<DB, TB>): OrmUpdateQueryBuilder<DB, TB, M> {
		return new OrmUpdateQueryBuilder(this._db, this._meta, this._table, this._inner.set(values as UpdateObject<DB, TB, TB>), this._wheres);
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
