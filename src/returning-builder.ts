import {
	Kysely,
	sql,
	type Selectable,
} from "kysely";
import { type MetaDB } from "./meta.js";

type AnyMeta = MetaDB<Record<string, any>>;

import type {
	RelationNames,
	RelationTarget,
	RelationToColumn,
	RelationResultType,
	RelationResultTypeWithModifier,
	RelationResultTypeWithVariant,
	ProjectionNames,
} from "./types.js";
import { type RelBuilder, type RelBuilderFactory } from "./relation-builder.js";
import type { OrmSelectQueryBuilder } from "./select-builder.js";
import {
	OrmRelationUpdateBuilder,
	OrmRelationDeleteBuilder,
	OrmRelationInsertBuilder,
	isRelationMutationBuilder,
} from "./relation-mutation-builder.js";
import {
	type RelationRequest,
	resolveWithRelatedArgs,
	buildToManyLateral,
	buildToOneSubquery,
	buildMutationCTEs,
	buildMutationCTERef,
	resetCTECounter,
} from "./helpers.js";

const CTE_ALIAS = "_mutation";
type ReturningSelection = string | readonly string[] | undefined;

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

function returningColumns(selection: ReturningSelection): string[] | null {
	if (!selection) {
		return null;
	}

	return (Array.isArray(selection) ? selection : [selection]).map((column) => {
		const parts = column.split(".");
		return parts[parts.length - 1] ?? column;
	});
}

function mergeReturningSelections(current: ReturningSelection, next: ReturningSelection): ReturningSelection {
	const currentColumns = returningColumns(current);
	if (!currentColumns) {
		return undefined;
	}

	return [...new Set([...currentColumns, ...(returningColumns(next) ?? [])])];
}

export class OrmReturningBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	O,
	M extends MetaDB<DB>,
> {
	constructor(
		private readonly _db: Kysely<DB>,
		private readonly _meta: M,
		private readonly _table: TB,
		private readonly _mutationQuery: any,
		private readonly _relations: RelationRequest[] = [],
		private readonly _rootWheres: any[][] = [],
		private readonly _returning: ReturningSelection = undefined,
	) {}

	returning<C extends ReturningColumn<DB, TB>>(
		column: C,
	): OrmReturningBuilder<DB, TB, O & ReturningOutput<DB, TB, C>, M>;
	returning<const C extends readonly ReturningColumn<DB, TB>[]>(
		columns: C,
	): OrmReturningBuilder<DB, TB, O & ReturningOutput<DB, TB, C[number]>, M>;
	returning(selection: ReturningColumn<DB, TB> | readonly ReturningColumn<DB, TB>[]): OrmReturningBuilder<DB, TB, any, M> {
		return new OrmReturningBuilder(
			this._db,
			this._meta,
			this._table,
			this._mutationQuery,
			this._relations,
			this._rootWheres,
			mergeReturningSelections(this._returning, selection),
		);
	}

	withRelated<R extends RelationNames<M, TB>>(
		name: R,
	): OrmReturningBuilder<DB, TB, O & RelationResultType<DB, M, TB, R>, M>;
	withRelated<R extends RelationNames<M, TB>, V extends string>(
		name: R,
		variant: V & ProjectionNames<M, RelationTarget<M, TB, R>>,
	): OrmReturningBuilder<DB, TB, O & RelationResultTypeWithVariant<DB, M, TB, R, R, V>, M>;
	withRelated<R extends RelationNames<M, TB>, RO>(
		name: R,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, RO, M> | OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>>,
	): OrmReturningBuilder<DB, TB, O & RelationResultTypeWithModifier<DB, M, TB, R, R, RO>, M>;
	withRelated<R extends RelationNames<M, TB>, A extends string>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
	): OrmReturningBuilder<DB, TB, O & RelationResultType<DB, M, TB, R, A>, M>;
	withRelated<R extends RelationNames<M, TB>, A extends string, V extends string>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
		variant: V & ProjectionNames<M, RelationTarget<M, TB, R>>,
	): OrmReturningBuilder<DB, TB, O & RelationResultTypeWithVariant<DB, M, TB, R, A, V>, M>;
	withRelated<R extends RelationNames<M, TB>, A extends string, RO>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, RO, M> | OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>>,
	): OrmReturningBuilder<DB, TB, O & RelationResultTypeWithModifier<DB, M, TB, R, A, RO>, M>;
	withRelated(
		nameOrBuilder: string | ((b: any) => RelBuilder<any, any, any, any, any>),
		variantOrModifier?: string | ((qb: any) => any),
	): OrmReturningBuilder<DB, TB, any, M> {
		const modifier = typeof variantOrModifier === "function" ? variantOrModifier : undefined;
		const variant = typeof variantOrModifier === "string" ? variantOrModifier : undefined;
		const request = resolveWithRelatedArgs(this._meta as AnyMeta, this._table, nameOrBuilder);
		if (variant) request.variant = variant;

		if (modifier) {
			const probeQb = this._createProbeQb(request.resolved.targetTable);
			const result = modifier(probeQb);
			if (isRelationMutationBuilder(result)) {
				request.mutation = result._toMutation();
			} else {
				request.modifier = modifier;
			}
		}

		return new OrmReturningBuilder(this._db, this._meta, this._table, this._mutationQuery, [...this._relations, request], this._rootWheres, this._returning);
	}

	mutateRelated(
		nameOrBuilder: string | ((b: any) => RelBuilder<any, any, any, any, any>),
		modifier: (qb: any) => any,
	): OrmReturningBuilder<DB, TB, O, M> {
		const request = resolveWithRelatedArgs(this._meta as AnyMeta, this._table, nameOrBuilder);
		request.fireAndForget = true;

		const probeQb = this._createProbeQb(request.resolved.targetTable);
		const result = modifier(probeQb);
		if (isRelationMutationBuilder(result)) {
			request.mutation = result._toMutation();
			if (request.mutation.nested.some(n => !n.fireAndForget)) {
				throw new Error("withRelated() cannot be used inside mutateRelated() — mutateRelated is fire-and-forget and does not return data. Use mutateRelated() for nested mutations instead.");
			}
		} else {
			throw new Error("mutateRelated callback must return .update(), .delete(), or .insert()");
		}

		return new OrmReturningBuilder(this._db, this._meta, this._table, this._mutationQuery, [...this._relations, request], this._rootWheres, this._returning);
	}

	/**
	 * Create a lightweight probe for detecting mutation vs read in callbacks.
	 * Avoids importing OrmSelectQueryBuilder (circular dependency).
	 * The probe only needs .update(), .delete(), .insert() to detect mutations,
	 * and acts as a passthrough Proxy for read modifiers (where, orderBy, etc.).
	 */
	private _createProbeQb(targetTable: string) {
		const meta = this._meta as AnyMeta;
		const inner = this._db.selectFrom(targetTable);
		const probe = {
			update: () => new OrmRelationUpdateBuilder(meta, targetTable),
			delete: () => new OrmRelationDeleteBuilder(meta, targetTable),
			insert: () => new OrmRelationInsertBuilder(meta, targetTable),
		};
		// For read modifiers: return a Proxy that delegates to the inner SelectQueryBuilder
		// and wraps results back in the probe shape
		return new Proxy(probe, {
			get(target, prop, receiver) {
				if (prop in target) return Reflect.get(target, prop, receiver);
				const innerVal = (inner as any)[prop];
				if (typeof innerVal !== "function") return innerVal;
				return (...args: any[]) => {
					const result = innerVal.apply(inner, args);
					// If it returns a query builder, wrap it back as a probe
					if (result && typeof result === "object" && typeof result.compile === "function") {
						return result;
					}
					return result;
				};
			},
		});
	}

	async execute(): Promise<O[]> {
		return await this._buildQuery().execute();
	}

	async executeTakeFirst(): Promise<O | undefined> {
		return (await this.execute())[0];
	}

	async executeTakeFirstOrThrow(error?: Error | (() => Error)): Promise<O> {
		const r = await this.executeTakeFirst();
		if (r === undefined) throw error instanceof Error ? error : typeof error === "function" ? error() : new Error(`No "${this._table}" found after mutation`);
		return r;
	}

	compile() {
		return this._buildQuery().compile();
	}

	/**
	 * Build the query.
	 *
	 * Without relations: just the mutation with RETURNING *.
	 * With relations: CTE wrapping the mutation, outer SELECT with subqueries.
	 *
	 * WITH <mutation_ctes...>, _mutation AS (<mutation> RETURNING *)
	 * SELECT _mutation.*, <relation subqueries>
	 * FROM _mutation
	 */
	private _buildQuery() {
		const requestedColumns = returningColumns(this._returning);

		if (this._relations.length === 0) {
			return requestedColumns
				? this._mutationQuery.returning(requestedColumns.map((column) => `${this._table}.${column}`))
				: this._mutationQuery.returningAll();
		}

		resetCTECounter();

		// Separate relations into reads and mutations
		const readRels: RelationRequest[] = [];
		const mutationRels: RelationRequest[] = [];
		for (const rel of this._relations) {
			if (rel.mutation) {
				mutationRels.push(rel);
			} else {
				readRels.push(rel);
			}
		}

		// Build mutation CTEs first
		let mutationCTEResult: ReturnType<typeof buildMutationCTEs> | null = null;
		if (mutationRels.length > 0) {
			mutationCTEResult = buildMutationCTEs(
				this._db, this._meta as AnyMeta,
				this._table, this._rootWheres,
				mutationRels,
			);
		}

		// Start building: add mutation CTEs, then the main mutation CTE, then SELECT
		let query: any = this._db;

		// Add relation mutation CTEs
		if (mutationCTEResult) {
			for (const cte of mutationCTEResult.ctes) {
				query = query.with(cte.name, () => cte.query);
			}
		}

		const cteColumns = requestedColumns ? new Set(requestedColumns) : null;
		if (cteColumns) {
			for (const rel of this._relations) {
				cteColumns.add(rel.resolved.fromColumn);
				for (const condition of rel.config.onConditions) {
					if (condition.kind === "ref") {
						cteColumns.add(condition.rhs);
					}
				}
			}
		}

		const returningMutation = cteColumns
			? this._mutationQuery.returning([...cteColumns].map((column) => `${this._table}.${column}`))
			: this._mutationQuery.returningAll();

		// Add the main mutation CTE
		query = query
			.with(CTE_ALIAS, () => returningMutation)
			.selectFrom(CTE_ALIAS);

		if (requestedColumns) {
			query = query.select(requestedColumns.map((column) => `${CTE_ALIAS}.${column}`));
		} else {
			query = query.selectAll(CTE_ALIAS);
		}

		// Add mutation CTE references
		if (mutationCTEResult) {
			for (const ref of mutationCTEResult.refs) {
				if (!ref.fireAndForget) {
					query = query.select(buildMutationCTERef(CTE_ALIAS, ref));
				}
			}
		}

		// Add read relations
		for (const rel of readRels) {
			if (rel.resolved.type === "many") {
				const { lateralExpr, latAlias, outputAlias } = buildToManyLateral(
					this._db, this._meta as AnyMeta, CTE_ALIAS, rel,
				);
				query = query.leftJoinLateral(lateralExpr, (join: any) => join.onTrue());
				query = query.select(sql.ref(`${latAlias}.data`).as(outputAlias));
			} else {
				query = query.select(buildToOneSubquery(this._db, this._meta as AnyMeta, CTE_ALIAS, rel));
			}
		}

		return query;
	}
}
