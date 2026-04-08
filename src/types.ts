import { type Selectable } from "kysely";

// ---------------------------------------------------------------------------
// Relation type utilities
// ---------------------------------------------------------------------------

export type TableRelations<M, T extends string> = T extends keyof M
	? M[T] extends { relations: infer R } ? R : {}
	: {};

export type RelationNames<M, T extends string> = keyof TableRelations<M, T> & string;

export type GetRelation<M, T extends string, R extends string> =
	R extends keyof TableRelations<M, T> ? TableRelations<M, T>[R] : never;

export type RelationTarget<M, T extends string, R extends string> =
	GetRelation<M, T, R> extends { model: infer Model } ? Model & string : never;

export type RelationType<M, T extends string, R extends string> =
	GetRelation<M, T, R> extends { type: infer Type } ? Type : never;

export type RelationToColumn<M, T extends string, R extends string> =
	GetRelation<M, T, R> extends { to: infer Col } ? Col & string : never;

// ---------------------------------------------------------------------------
// Projection type utilities
// ---------------------------------------------------------------------------

export type HasProjections<M, T extends string> = T extends keyof M
	? M[T] extends { projections: infer P }
		? P extends Record<string, any> ? true : false
		: false
	: false;

export type ProjectionNames<M, T extends string> = T extends keyof M
	? M[T] extends { projections: infer P }
		? keyof P & string
		: never
	: never;

export type ProjectionColumns<M, T extends string, V extends string> = T extends keyof M
	? M[T] extends { projections: infer P }
		? V extends keyof P
			? P[V] extends ReadonlyArray<infer C> ? C & string : never
			: never
		: never
	: never;

export type ProjectionResult<
	DB extends Record<string, any>, M, T extends string, V extends string,
> = Pick<Selectable<DB[T & keyof DB]>, ProjectionColumns<M, T, V> & keyof Selectable<DB[T & keyof DB]>>;

export type DefaultTableOutput<
	DB extends Record<string, any>, M, TB extends string,
> = HasProjections<M, TB> extends true
	? "default" extends ProjectionNames<M, TB>
		? ProjectionResult<DB, M, TB, "default">
		: {}
	: Selectable<DB[TB]>;

export type DefaultRelationOutput<
	DB extends Record<string, any>, M, Target extends string,
> = HasProjections<M, Target> extends true
	? "relation" extends ProjectionNames<M, Target>
		? ProjectionResult<DB, M, Target, "relation">
		: "default" extends ProjectionNames<M, Target>
			? ProjectionResult<DB, M, Target, "default">
			: Selectable<DB[Target & keyof DB]>
	: Selectable<DB[Target & keyof DB]>;

// ---------------------------------------------------------------------------
// Composite result types
// ---------------------------------------------------------------------------

/** Merge Extra into Base, but if Extra is `any` (from `(qb: any) =>` callbacks), return Base unchanged. */
type MergeRelOutput<Base, Extra> = 0 extends (1 & Extra) ? Base : Base & Extra;

export type RelationResultType<
	DB extends Record<string, any>, M, T extends string, R extends string, Alias extends string = R,
> = RelationType<M, T, R> extends "many"
	? { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? DefaultRelationOutput<DB, M, RelationTarget<M, T, R>>[] : never }
	: { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? DefaultRelationOutput<DB, M, RelationTarget<M, T, R>> | null : never };

export type RelationResultTypeWithModifier<
	DB extends Record<string, any>, M, T extends string, R extends string, Alias extends string, RO,
> = RelationType<M, T, R> extends "many"
	? { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? MergeRelOutput<DefaultRelationOutput<DB, M, RelationTarget<M, T, R>>, RO>[] : never }
	: { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? MergeRelOutput<DefaultRelationOutput<DB, M, RelationTarget<M, T, R>>, RO> | null : never };

export type RelationResultTypeWithVariant<
	DB extends Record<string, any>, M, T extends string, R extends string, Alias extends string, V extends string,
> = RelationType<M, T, R> extends "many"
	? { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? ProjectionResult<DB, M, RelationTarget<M, T, R>, V>[] : never }
	: { [K in Alias]: RelationTarget<M, T, R> extends keyof DB ? ProjectionResult<DB, M, RelationTarget<M, T, R>, V> | null : never };

export type ResolveOutput<
	DB extends Record<string, any>, M, TB extends keyof DB & string, O, S extends boolean,
> = S extends true ? O : DefaultTableOutput<DB, M, TB> & O;
