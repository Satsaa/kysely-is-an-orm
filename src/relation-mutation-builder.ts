import {
	type Insertable,
	type Updateable,
	type ReferenceExpression,
	type ComparisonOperatorExpression,
	type OperandValueExpressionOrList,
} from "kysely";
import { type MetaDB } from "./meta.js";
import type {
	RelationNames,
	RelationTarget,
	RelationResultType,
	ProjectionNames,
} from "./types.js";
import { type RelBuilder, type RelBuilderFactory } from "./relation-builder.js";

// ---------------------------------------------------------------------------
// Shared types — stored on mutation builders, read at CTE build time
// ---------------------------------------------------------------------------

/** A nested relation inside a mutation builder (withRelated/mutateRelated on update/insert) */
export interface NestedRelationRef {
	nameOrBuilder: string | ((b: any) => any);
	modifier?: (qb: any) => any;
	variant?: string;
	fireAndForget: boolean;
}

/** Extracted mutation config — produced by _toMutation() */
export interface RelationMutation {
	kind: "update" | "delete" | "insert";
	set?: Updateable<any>;
	values?: Insertable<any> | Insertable<any>[];
	onConflict?: (oc: any) => any;
	wheres: any[][];
	projection?: string;
	nested: NestedRelationRef[];
}

// ---------------------------------------------------------------------------
// OrmRelationUpdateBuilder
// ---------------------------------------------------------------------------

export class OrmRelationUpdateBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
	O = {},
	EC extends string = never,
> {
	readonly _kind = "update" as const;

	constructor(
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _set: Updateable<DB[TB]> | null = null,
		private readonly _wheres: any[][] = [],
		private readonly _projection: string | null = null,
		private readonly _nested: NestedRelationRef[] = [],
	) {}

	set(values: Updateable<DB[TB]>): OrmRelationUpdateBuilder<DB, TB, M, O, EC> {
		return new OrmRelationUpdateBuilder(this._meta, this._table, values, this._wheres, this._projection, this._nested);
	}

	where<RE extends Exclude<ReferenceExpression<DB, TB>, EC>>(
		lhs: RE, op: ComparisonOperatorExpression, rhs: OperandValueExpressionOrList<DB, TB, RE>,
	): OrmRelationUpdateBuilder<DB, TB, M, O, EC>;
	where(...args: any[]): OrmRelationUpdateBuilder<DB, TB, M, O, EC> {
		return new OrmRelationUpdateBuilder(this._meta, this._table, this._set, [...this._wheres, args], this._projection, this._nested);
	}

	project<V extends ProjectionNames<M, TB>>(variant: V): OrmRelationUpdateBuilder<DB, TB, M, O, EC> {
		return new OrmRelationUpdateBuilder(this._meta, this._table, this._set, this._wheres, variant as string, this._nested);
	}

	withRelated<R extends RelationNames<M, TB>>(name: R): OrmRelationUpdateBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated<R extends RelationNames<M, TB>>(name: R, variant: string): OrmRelationUpdateBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated<R extends RelationNames<M, TB>>(name: R, modifier: (qb: any) => any): OrmRelationUpdateBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated(builder: (b: any) => any): OrmRelationUpdateBuilder<DB, TB, M, O, EC>;
	withRelated(builder: (b: any) => any, modifier: (qb: any) => any): OrmRelationUpdateBuilder<DB, TB, M, O, EC>;
	withRelated(builder: (b: any) => any, variant: string): OrmRelationUpdateBuilder<DB, TB, M, O, EC>;
	withRelated(
		nameOrBuilder: string | ((b: any) => any),
		variantOrModifier?: string | ((qb: any) => any),
	): OrmRelationUpdateBuilder<DB, TB, M, any, EC> {
		const ref: NestedRelationRef = {
			nameOrBuilder,
			modifier: typeof variantOrModifier === "function" ? variantOrModifier : undefined,
			variant: typeof variantOrModifier === "string" ? variantOrModifier : undefined,
			fireAndForget: false,
		};
		return new OrmRelationUpdateBuilder(this._meta, this._table, this._set, this._wheres, this._projection, [...this._nested, ref]);
	}

	mutateRelated(
		nameOrBuilder: string | ((b: any) => any),
		modifier: (qb: any) => any,
	): OrmRelationUpdateBuilder<DB, TB, M, O, EC> {
		const ref: NestedRelationRef = { nameOrBuilder, modifier, fireAndForget: true };
		return new OrmRelationUpdateBuilder(this._meta, this._table, this._set, this._wheres, this._projection, [...this._nested, ref]);
	}

	/** @internal */
	_toMutation(): RelationMutation {
		return {
			kind: "update",
			set: this._set!,
			wheres: this._wheres,
			projection: this._projection ?? undefined,
			nested: this._nested,
		};
	}
}

// ---------------------------------------------------------------------------
// OrmRelationDeleteBuilder
// ---------------------------------------------------------------------------

export class OrmRelationDeleteBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
	EC extends string = never,
> {
	readonly _kind = "delete" as const;

	constructor(
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _wheres: any[][] = [],
	) {}

	where<RE extends Exclude<ReferenceExpression<DB, TB>, EC>>(
		lhs: RE, op: ComparisonOperatorExpression, rhs: OperandValueExpressionOrList<DB, TB, RE>,
	): OrmRelationDeleteBuilder<DB, TB, M, EC>;
	where(...args: any[]): OrmRelationDeleteBuilder<DB, TB, M, EC> {
		return new OrmRelationDeleteBuilder(this._meta, this._table, [...this._wheres, args]);
	}

	/** @internal */
	_toMutation(): RelationMutation {
		return { kind: "delete", wheres: this._wheres, nested: [] };
	}
}

// ---------------------------------------------------------------------------
// OrmRelationInsertBuilder
// ---------------------------------------------------------------------------

export class OrmRelationInsertBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	M extends MetaDB<DB>,
	O = {},
	EC extends string = never,
> {
	readonly _kind = "insert" as const;

	constructor(
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _values: Insertable<DB[TB]> | Insertable<DB[TB]>[] | null = null,
		private readonly _onConflict: ((oc: any) => any) | null = null,
		private readonly _projection: string | null = null,
		private readonly _nested: NestedRelationRef[] = [],
	) {}

	values(values: Omit<Insertable<DB[TB]>, EC> | ReadonlyArray<Omit<Insertable<DB[TB]>, EC>>): OrmRelationInsertBuilder<DB, TB, M, O, EC> {
		return new OrmRelationInsertBuilder(this._meta, this._table, values as any, this._onConflict, this._projection, this._nested);
	}

	onConflict(handler: (oc: any) => any): OrmRelationInsertBuilder<DB, TB, M, O, EC> {
		return new OrmRelationInsertBuilder(this._meta, this._table, this._values, handler, this._projection, this._nested);
	}

	project<V extends ProjectionNames<M, TB>>(variant: V): OrmRelationInsertBuilder<DB, TB, M, O, EC> {
		return new OrmRelationInsertBuilder(this._meta, this._table, this._values, this._onConflict, variant as string, this._nested);
	}

	withRelated<R extends RelationNames<M, TB>>(name: R): OrmRelationInsertBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated<R extends RelationNames<M, TB>>(name: R, variant: string): OrmRelationInsertBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated<R extends RelationNames<M, TB>>(name: R, modifier: (qb: any) => any): OrmRelationInsertBuilder<DB, TB, M, O & RelationResultType<DB, M, TB, R>, EC>;
	withRelated(builder: (b: any) => any): OrmRelationInsertBuilder<DB, TB, M, O, EC>;
	withRelated(builder: (b: any) => any, modifier: (qb: any) => any): OrmRelationInsertBuilder<DB, TB, M, O, EC>;
	withRelated(builder: (b: any) => any, variant: string): OrmRelationInsertBuilder<DB, TB, M, O, EC>;
	withRelated(
		nameOrBuilder: string | ((b: any) => any),
		variantOrModifier?: string | ((qb: any) => any),
	): OrmRelationInsertBuilder<DB, TB, M, any, EC> {
		const ref: NestedRelationRef = {
			nameOrBuilder,
			modifier: typeof variantOrModifier === "function" ? variantOrModifier : undefined,
			variant: typeof variantOrModifier === "string" ? variantOrModifier : undefined,
			fireAndForget: false,
		};
		return new OrmRelationInsertBuilder(this._meta, this._table, this._values, this._onConflict, this._projection, [...this._nested, ref]);
	}

	mutateRelated(
		nameOrBuilder: string | ((b: any) => any),
		modifier: (qb: any) => any,
	): OrmRelationInsertBuilder<DB, TB, M, O, EC> {
		const ref: NestedRelationRef = { nameOrBuilder, modifier, fireAndForget: true };
		return new OrmRelationInsertBuilder(this._meta, this._table, this._values, this._onConflict, this._projection, [...this._nested, ref]);
	}

	/** @internal */
	_toMutation(): RelationMutation {
		return {
			kind: "insert",
			values: this._values!,
			onConflict: this._onConflict ?? undefined,
			wheres: [],
			projection: this._projection ?? undefined,
			nested: this._nested,
		};
	}
}

// ---------------------------------------------------------------------------
// Type guard
// ---------------------------------------------------------------------------

type AnyRelationMutationBuilder =
	| OrmRelationUpdateBuilder<any, any, any, any, any>
	| OrmRelationDeleteBuilder<any, any, any, any>
	| OrmRelationInsertBuilder<any, any, any, any, any>;

export function isRelationMutationBuilder(value: unknown): value is AnyRelationMutationBuilder {
	return value != null && typeof value === "object" && "_kind" in value
		&& ((value as any)._kind === "update" || (value as any)._kind === "delete" || (value as any)._kind === "insert");
}
