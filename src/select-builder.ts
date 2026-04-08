import {
	Kysely,
	SelectQueryBuilder,
	sql,
	type ReferenceExpression,
	type ComparisonOperatorExpression,
	type OperandValueExpressionOrList,
	type ExpressionOrFactory,
	type SqlBool,
	type OrderByExpression,
	type OrderByDirectionExpression,
	type SelectExpression,
	type SelectCallback,
	type Selection,
	type CallbackSelection,
	type Selectable,
} from "kysely";
import { type MetaDB } from "./meta.js";
import type {
	RelationNames,
	RelationTarget,
	RelationToColumn,
	RelationResultType,
	RelationResultTypeWithModifier,
	RelationResultTypeWithVariant,
	ResolveOutput,
	ProjectionNames,
	ProjectionResult,
} from "./types.js";
import { type RelBuilder, type RelBuilderFactory } from "./relation-builder.js";
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
	buildMutationCTEs,
	buildMutationCTERef,
	resetCTECounter,
} from "./helpers.js";

type AnyMeta = MetaDB<Record<string, any>>;

export class OrmSelectQueryBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	O,
	M extends MetaDB<DB>,
	S extends boolean = false,
	EC extends string = never,
> {
	readonly _inner: SelectQueryBuilder<DB, TB, O>;

	constructor(
		private readonly _db: Kysely<DB>,
		private readonly _meta: M,
		private readonly _table: TB,
		inner: SelectQueryBuilder<DB, TB, O>,
		private readonly _relations: RelationRequest[] = [],
		private readonly _hasExplicitSelect: boolean = false,
		private readonly _fallbackVariants: string[] = ["default"],
		private readonly _wheres: any[][] = [],
	) {
		this._inner = inner;
		return new Proxy(this, {
			get(target, prop, receiver) {
				if (prop in target || typeof prop === "symbol") return Reflect.get(target, prop, receiver);
				const innerVal = (target._inner as any)[prop];
				if (typeof innerVal !== "function") return innerVal;
				return (...args: any[]) => {
					const result = innerVal.apply(target._inner, args);
					if (result && typeof result === "object" && typeof result.compile === "function") {
						return new OrmSelectQueryBuilder(target._db, target._meta, target._table, result, target._relations, target._hasExplicitSelect, target._fallbackVariants, target._wheres);
					}
					return result;
				};
			},
		});
	}

	where<RE extends ReferenceExpression<DB, TB>>(
		lhs: RE, op: ComparisonOperatorExpression, rhs: OperandValueExpressionOrList<DB, TB, RE>,
	): OrmSelectQueryBuilder<DB, TB, O, M, S, EC>;
	where<E extends ExpressionOrFactory<DB, TB, SqlBool>>(expression: E): OrmSelectQueryBuilder<DB, TB, O, M, S, EC>;
	where(...args: any[]): OrmSelectQueryBuilder<DB, TB, O, M, S, EC> {
		if (args.length >= 2 && typeof args[0] === 'string' && !args[0].includes('.')) {
			args[0] = `${this._table}.${args[0]}`;
		}
		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, (this._inner as any).where(...args), this._relations, this._hasExplicitSelect, this._fallbackVariants, [...this._wheres, args]);
	}

	orderBy<OE extends OrderByExpression<DB, TB, O>>(orderBy: OE, direction?: OrderByDirectionExpression): OrmSelectQueryBuilder<DB, TB, O, M, S, EC>;
	orderBy(...args: any[]): OrmSelectQueryBuilder<DB, TB, O, M, S, EC> {
		if (args.length >= 1 && typeof args[0] === 'string' && !args[0].includes('.')) {
			args[0] = `${this._table}.${args[0]}`;
		}
		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, (this._inner as any).orderBy(...args), this._relations, this._hasExplicitSelect, this._fallbackVariants, this._wheres);
	}

	select<SE extends SelectExpression<DB, TB>>(selections: ReadonlyArray<SE>): OrmSelectQueryBuilder<DB, TB, O & Selection<DB, TB, SE>, M, true, EC>;
	select<CB extends SelectCallback<DB, TB>>(callback: CB): OrmSelectQueryBuilder<DB, TB, O & CallbackSelection<DB, TB, CB>, M, true, EC>;
	select<SE extends SelectExpression<DB, TB>>(selection: SE): OrmSelectQueryBuilder<DB, TB, O & Selection<DB, TB, SE>, M, true, EC>;
	select(selection: SelectExpression<DB, TB> | ReadonlyArray<SelectExpression<DB, TB>> | SelectCallback<DB, TB>): OrmSelectQueryBuilder<DB, TB, any, M, true, EC> {
		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, this._inner.select(selection as any), this._relations, true, this._fallbackVariants, this._wheres);
	}

	selectAll(): OrmSelectQueryBuilder<DB, TB, Selectable<DB[TB]>, M, true, EC> {
		return new OrmSelectQueryBuilder<DB, TB, Selectable<DB[TB]>, M, true, EC>(this._db, this._meta, this._table, this._inner.selectAll(this._table), this._relations, true, this._fallbackVariants, this._wheres);
	}

	project<V extends ProjectionNames<M, TB>>(
		variant: V,
	): OrmSelectQueryBuilder<DB, TB, O & ProjectionResult<DB, M, TB, V>, M, true, EC> {
		const tableMeta = (this._meta as AnyMeta)[this._table];
		const columns = tableMeta?.projections?.[variant as string];
		if (!columns) throw new Error(`Projection "${variant}" not found on table "${this._table}"`);
		const qualified = columns.map((c) => `${this._table}.${c}`);
		return new OrmSelectQueryBuilder(
			this._db, this._meta, this._table,
			this._inner.select(qualified),
			this._relations, true, this._fallbackVariants, this._wheres,
		) as any;
	}

	limit(limit: number | bigint): OrmSelectQueryBuilder<DB, TB, O, M, S, EC> {
		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, this._inner.limit(limit), this._relations, this._hasExplicitSelect, this._fallbackVariants, this._wheres);
	}

	offset(offset: number | bigint): OrmSelectQueryBuilder<DB, TB, O, M, S, EC> {
		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, this._inner.offset(offset), this._relations, this._hasExplicitSelect, this._fallbackVariants, this._wheres);
	}

	// -----------------------------------------------------------------------
	// Relation mutation mode switches (for use inside withRelated callbacks)
	// -----------------------------------------------------------------------

	update(): OrmRelationUpdateBuilder<DB, TB, M, {}, EC> {
		return new OrmRelationUpdateBuilder(this._meta, this._table);
	}

	delete(): OrmRelationDeleteBuilder<DB, TB, M, EC> {
		return new OrmRelationDeleteBuilder(this._meta, this._table);
	}

	insert(): OrmRelationInsertBuilder<DB, TB, M, {}, EC> {
		return new OrmRelationInsertBuilder(this._meta, this._table);
	}

	// -----------------------------------------------------------------------
	// withRelated
	// -----------------------------------------------------------------------

	withRelated<R extends RelationNames<M, TB>>(
		name: R,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultType<DB, M, TB, R>, M, S, EC>;
	withRelated<R extends RelationNames<M, TB>, V extends string>(
		name: R,
		variant: V & ProjectionNames<M, RelationTarget<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultTypeWithVariant<DB, M, TB, R, R, V>, M, S, EC>;
	withRelated<R extends RelationNames<M, TB>, RO>(
		name: R,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, RO, M> | OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultTypeWithModifier<DB, M, TB, R, R, RO>, M, S, EC>;
	withRelated<R extends RelationNames<M, TB>, A extends string>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultType<DB, M, TB, R, A>, M, S, EC>;
	withRelated<R extends RelationNames<M, TB>, A extends string, V extends string>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
		variant: V & ProjectionNames<M, RelationTarget<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultTypeWithVariant<DB, M, TB, R, A, V>, M, S, EC>;
	withRelated<R extends RelationNames<M, TB>, A extends string, RO>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, RO, M> | OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, RO, RelationToColumn<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O & RelationResultTypeWithModifier<DB, M, TB, R, A, RO>, M, S, EC>;
	withRelated(
		nameOrBuilder: string | ((b: any) => RelBuilder<any, any, any, any, any>),
		variantOrModifier?: string | ((qb: any) => any),
	): OrmSelectQueryBuilder<DB, TB, any, M> {
		const modifier = typeof variantOrModifier === "function" ? variantOrModifier : undefined;
		const variant = typeof variantOrModifier === "string" ? variantOrModifier : undefined;
		const request = resolveWithRelatedArgs(this._meta as AnyMeta, this._table, nameOrBuilder);
		if (variant) request.variant = variant;

		if (modifier) {
			// Probe: call modifier with a builder for the target table to detect mutation vs read
			const probeQb = new OrmSelectQueryBuilder(
				this._db, this._meta, request.resolved.targetTable as TB,
				this._db.selectFrom(request.resolved.targetTable) as any,
				[], false, ["relation", "default"],
			);
			const result = modifier(probeQb);
			if (isRelationMutationBuilder(result)) {
				request.mutation = result._toMutation();
			} else {
				request.modifier = modifier;
			}
		}

		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, this._inner, [...this._relations, request], this._hasExplicitSelect, this._fallbackVariants, this._wheres);
	}

	// -----------------------------------------------------------------------
	// mutateRelated (fire-and-forget)
	// -----------------------------------------------------------------------

	mutateRelated<R extends RelationNames<M, TB>>(
		name: R,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, any, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, any, RelationToColumn<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O, M, S, EC>;
	mutateRelated<R extends RelationNames<M, TB>, A extends string>(
		builder: (b: RelBuilderFactory<DB, M, TB>) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>, A>,
		modifier: (qb: OrmSelectQueryBuilder<DB, RelationTarget<M, TB, R>, {}, M, false, RelationToColumn<M, TB, R>>) => OrmRelationUpdateBuilder<DB, RelationTarget<M, TB, R>, M, any, RelationToColumn<M, TB, R>> | OrmRelationDeleteBuilder<DB, RelationTarget<M, TB, R>, M, RelationToColumn<M, TB, R>> | OrmRelationInsertBuilder<DB, RelationTarget<M, TB, R>, M, any, RelationToColumn<M, TB, R>>,
	): OrmSelectQueryBuilder<DB, TB, O, M, S, EC>;
	mutateRelated(
		nameOrBuilder: string | ((b: any) => RelBuilder<any, any, any, any, any>),
		modifier: (qb: any) => any,
	): OrmSelectQueryBuilder<DB, TB, any, M> {
		const request = resolveWithRelatedArgs(this._meta as AnyMeta, this._table, nameOrBuilder);
		request.fireAndForget = true;

		// Probe the modifier
		const probeQb = new OrmSelectQueryBuilder(
			this._db, this._meta, request.resolved.targetTable as TB,
			this._db.selectFrom(request.resolved.targetTable) as any,
			[], false, ["relation", "default"],
		);
		const result = modifier(probeQb);
		if (isRelationMutationBuilder(result)) {
			request.mutation = result._toMutation();
			if (request.mutation.nested.some(n => !n.fireAndForget)) {
				throw new Error("withRelated() cannot be used inside mutateRelated() — mutateRelated is fire-and-forget and does not return data. Use mutateRelated() for nested mutations instead.");
			}
		} else {
			throw new Error("mutateRelated callback must return .update(), .delete(), or .insert()");
		}

		return new OrmSelectQueryBuilder(this._db, this._meta, this._table, this._inner, [...this._relations, request], this._hasExplicitSelect, this._fallbackVariants, this._wheres);
	}

	// -----------------------------------------------------------------------
	// Execution
	// -----------------------------------------------------------------------

	async execute(): Promise<ResolveOutput<DB, M, TB, O, S>[]> {
		return (await this._buildFinalQuery().execute()) as ResolveOutput<DB, M, TB, O, S>[];
	}
	async executeTakeFirst(): Promise<ResolveOutput<DB, M, TB, O, S> | undefined> {
		return (await this.limit(1).execute())[0];
	}
	async executeTakeFirstOrThrow(error?: Error | (() => Error)): Promise<ResolveOutput<DB, M, TB, O, S>> {
		const r = await this.executeTakeFirst();
		if (r === undefined) throw error instanceof Error ? error : typeof error === "function" ? error() : new Error(`No "${this._table}" found`);
		return r;
	}
	compile() { return this._buildFinalQuery().compile(); }

	/** @internal */
	_buildFinalQuery(): SelectQueryBuilder<DB, TB, any> {
		resetCTECounter();

		let query = this._inner as SelectQueryBuilder<DB, TB, any>;

		// Apply default projection if no explicit select
		if (!this._hasExplicitSelect) {
			const tableMeta = (this._meta as AnyMeta)[this._table];
			if (tableMeta?.projections) {
				for (const variant of this._fallbackVariants) {
					const columns = tableMeta.projections[variant];
					if (columns) {
						const qualified = columns.map((c) => `${this._table}.${c}`);
						query = query.select(qualified) as any;
						break;
					}
				}
			} else {
				query = query.selectAll(this._table) as any;
			}
		}

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

		// Build mutation CTEs (if any)
		// SelectQueryBuilder doesn't have .with() — CTEs must start from the Kysely instance.
		// When mutations exist, wrap the inner query as a "_main" CTE and select from it.
		if (mutationRels.length > 0) {
			const { ctes, refs } = buildMutationCTEs(
				this._db, this._meta as AnyMeta,
				this._table, this._wheres,
				mutationRels,
			);

			const MAIN_CTE = "_main";

			// Start from db, add mutation CTEs, then wrap inner query as _main
			let cteQuery: any = this._db;
			for (const cte of ctes) {
				cteQuery = cteQuery.with(cte.name, () => cte.query);
			}
			cteQuery = cteQuery.with(MAIN_CTE, () => query);

			// Select from _main
			let outerQuery: any = cteQuery.selectFrom(MAIN_CTE).selectAll(MAIN_CTE);

			// Add scalar subquery references for non-fire-and-forget mutations
			for (const ref of refs) {
				if (!ref.fireAndForget) {
					outerQuery = outerQuery.select(buildMutationCTERef(MAIN_CTE, ref));
				}
			}

			// Build read relations against _main
			for (const rel of readRels) {
				if (rel.resolved.type === "one") {
					outerQuery = this._buildJoinRelation(outerQuery, rel, MAIN_CTE);
				} else {
					const { lateralExpr, latAlias, outputAlias } = buildToManyLateral(
						this._db, this._meta as AnyMeta, MAIN_CTE, rel,
					);
					outerQuery = outerQuery.leftJoinLateral(lateralExpr, (join: any) => join.onTrue());
					outerQuery = outerQuery.select(sql.ref(`${latAlias}.data`).as(outputAlias));
				}
			}

			return outerQuery;
		}

		// No mutations: existing LATERAL/JOIN logic (unchanged)
		for (const rel of readRels) {
			if (rel.resolved.type === "one") {
				query = this._buildJoinRelation(query, rel, this._table);
			} else {
				// toMany: LEFT JOIN LATERAL (read)
				const { lateralExpr, latAlias, outputAlias } = buildToManyLateral(
					this._db, this._meta as AnyMeta, this._table, rel,
				);
				query = (query as any).leftJoinLateral(lateralExpr, (join: any) => join.onTrue());
				query = query.select(sql.ref(`${latAlias}.data`).as(outputAlias)) as any;
			}
		}

		return query;
	}

	private _buildJoinRelation(query: SelectQueryBuilder<DB, TB, any>, req: RelationRequest, sourceRef: string): SelectQueryBuilder<DB, TB, any> {
		const { config, resolved } = req;
		const joinAlias = `_rel_${config.alias}`;
		const joinFn = config.joinType === "inner" ? "innerJoin" : "leftJoin";

		query = (query as any)[joinFn](
			`${resolved.targetTable} as ${joinAlias}`,
			(join: any) => {
				let j = join.onRef(`${joinAlias}.${resolved.toColumn}`, "=", `${sourceRef}.${resolved.fromColumn}`);
				for (const c of config.onConditions) {
					j = c.kind === "ref"
						? j.onRef(`${joinAlias}.${c.lhs}`, c.op, `${sourceRef}.${c.rhs}`)
						: j.on(`${joinAlias}.${c.lhs}`, c.op, c.rhs);
				}
				return j;
			},
		);

		// Determine which columns to include in the jsonb output
		const projColumns = this._getRelationProjectionColumns(resolved.targetTable, req.variant);

		const fromRef = sql.ref(`${sourceRef}.${resolved.fromColumn}`);
		if (projColumns) {
			// Build jsonb_build_object('col1', alias.col1, 'col2', alias.col2, ...)
			const parts = projColumns.map(
				(c) => sql`${sql.lit(c)}, ${sql.ref(`${joinAlias}.${c}`)}`,
			);
			const jsonbExpr = sql`jsonb_build_object(${sql.join(parts)})`;
			if (config.joinType === "inner") {
				query = query.select(jsonbExpr.as(config.alias)) as any;
			} else {
				query = query.select(sql`CASE WHEN ${fromRef} IS NULL THEN NULL ELSE ${jsonbExpr} END`.as(config.alias)) as any;
			}
		} else {
			const joinRef = sql.ref(joinAlias);
			if (config.joinType === "inner") {
				query = query.select(sql`to_jsonb(${joinRef}.*)`.as(config.alias)) as any;
			} else {
				query = query.select(sql`CASE WHEN ${fromRef} IS NULL THEN NULL ELSE to_jsonb(${joinRef}.*) END`.as(config.alias)) as any;
			}
		}
		return query;
	}

	/** Get projection columns for a relation target, or null for selectAll. */
	private _getRelationProjectionColumns(targetTable: string, variant?: string): string[] | null {
		const tableMeta = (this._meta as AnyMeta)[targetTable];
		if (variant) {
			const columns = tableMeta?.projections?.[variant];
			return columns ? [...columns] : null;
		}
		if (tableMeta?.projections) {
			const cols = tableMeta.projections["relation"] || tableMeta.projections["default"];
			return cols ? [...cols] : null;
		}
		return null; // no projections = selectAll
	}
}
