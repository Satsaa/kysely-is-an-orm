import { Kysely, sql } from "kysely";
import { type MetaDB, type ResolvedRelation, resolveRelation } from "./meta.js";
import { type RelBuilderConfig, type RelBuilderFactory, RelBuilder, createRelBuilderFactory } from "./relation-builder.js";
import { OrmSelectQueryBuilder } from "./select-builder.js";
import {
	type RelationMutation,
	type NestedRelationRef,
	isRelationMutationBuilder,
} from "./relation-mutation-builder.js";

// All meta parameters use MetaDB<Record<string, any>> — this gives string keys
// and ReadonlyArray<string> projections while avoiding the symbol-key issues of MetaDB<any>.
type AnyMeta = MetaDB<Record<string, any>>;

// ---------------------------------------------------------------------------
// RelationRequest
// ---------------------------------------------------------------------------

export interface RelationRequest {
	config: RelBuilderConfig;
	resolved: ResolvedRelation;
	// Read mode:
	modifier?: (qb: OrmSelectQueryBuilder<any, any, any, any>) => OrmSelectQueryBuilder<any, any, any, any>;
	variant?: string;
	// Mutation mode:
	mutation?: RelationMutation;
	fireAndForget?: boolean;
}

export function resolveWithRelatedArgs(
	meta: AnyMeta, table: string,
	nameOrBuilder: string | ((b: RelBuilderFactory<any, any, any>) => RelBuilder<any, any, any, any, any>),
	modifier?: (qb: OrmSelectQueryBuilder<any, any, any, any>) => OrmSelectQueryBuilder<any, any, any, any>,
): RelationRequest {
	let config: RelBuilderConfig;
	if (typeof nameOrBuilder === "string") {
		config = { name: nameOrBuilder, alias: nameOrBuilder, joinType: "left", onConditions: [] };
	} else {
		config = nameOrBuilder(createRelBuilderFactory())._config;
	}
	const tableMeta = meta[table];
	const relDef = tableMeta?.relations?.[config.name];
	if (!relDef) throw new Error(`Relation "${config.name}" not found on table "${table}"`);
	return { config, resolved: resolveRelation(relDef), modifier };
}

// ---------------------------------------------------------------------------
// LATERAL builders (read-only relations — unchanged)
// ---------------------------------------------------------------------------

/**
 * Build the inner query for a toMany relation (used inside LATERAL).
 * Handles correlation, through-tables, modifiers, nested withRelated.
 */
export function buildToManyInnerQuery(
	db: Kysely<any>, meta: AnyMeta, sourceRef: string, req: RelationRequest,
) {
	const { config, resolved } = req;
	const modifier = typeof req.modifier === "function" ? req.modifier : undefined;
	const targetTable = resolved.targetTable;

	let inner = new OrmSelectQueryBuilder(
		db, meta, targetTable,
		db.selectFrom(targetTable),
		[],
		false,
		["relation", "default"],
	);

	// Apply explicit variant if specified on the relation request
	if (req.variant) {
		const tableMeta = meta[targetTable];
		const columns = tableMeta?.projections?.[req.variant];
		if (!columns) throw new Error(`Projection "${req.variant}" not found on table "${targetTable}"`);
		const qualified = columns.map((c) => `${targetTable}.${c}`);
		inner = new OrmSelectQueryBuilder(
			db, meta, targetTable,
			inner._inner.select(qualified),
			[],
			true,
			["relation", "default"],
		);
	}

	if (modifier) inner = modifier(inner);
	let innerQuery = inner._buildFinalQuery();

	if (resolved.through) {
		const t = resolved.through;
		innerQuery = innerQuery
			.innerJoin(t.table, `${t.table}.${t.to}`, `${targetTable}.${resolved.toColumn}`)
			.whereRef(`${t.table}.${t.from}`, "=", `${sourceRef}.${resolved.fromColumn}`);
	} else {
		innerQuery = innerQuery.whereRef(
			`${targetTable}.${resolved.toColumn}`, "=",
			`${sourceRef}.${resolved.fromColumn}`,
		);
	}
	for (const cond of config.onConditions) {
		if (cond.kind === "ref") {
			innerQuery = innerQuery.whereRef(`${targetTable}.${cond.lhs}`, cond.op, `${sourceRef}.${cond.rhs}`);
		} else {
			innerQuery = innerQuery.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
		}
	}
	return innerQuery;
}

/**
 * Build a toMany LATERAL expression + alias.
 */
export function buildToManyLateral(
	db: Kysely<any>, meta: AnyMeta, sourceRef: string, req: RelationRequest,
) {
	const innerQuery = buildToManyInnerQuery(db, meta, sourceRef, req);
	const latAlias = `_lat_${req.config.alias}`;
	return {
		lateralExpr: sql`(SELECT COALESCE(jsonb_agg(to_jsonb(x)), '[]'::jsonb) as data FROM (${innerQuery}) x)`.as(latAlias),
		latAlias,
		outputAlias: req.config.alias,
	};
}

/**
 * Build a toOne relation as a scalar subquery with LIMIT 1.
 * Used in CTE/RETURNING context where JOINs aren't available.
 */
export function buildToOneSubquery(
	db: Kysely<any>, meta: AnyMeta, sourceRef: string, req: RelationRequest,
) {
	const { config, resolved } = req;
	const targetTable = resolved.targetTable;

	let inner = db.selectFrom(targetTable);

	// Apply projection
	const tableMeta = meta[targetTable];
	if (req.variant) {
		const columns = tableMeta?.projections?.[req.variant];
		if (columns) {
			inner = inner.select(columns.map((c) => `${targetTable}.${c}`));
		} else {
			inner = inner.selectAll(targetTable);
		}
	} else {
		if (tableMeta?.projections) {
			const relCols = tableMeta.projections["relation"] || tableMeta.projections["default"];
			if (relCols) {
				inner = inner.select(relCols.map((c) => `${targetTable}.${c}`));
			} else {
				inner = inner.selectAll(targetTable);
			}
		} else {
			inner = inner.selectAll(targetTable);
		}
	}

	// Correlation
	inner = inner.whereRef(
		`${targetTable}.${resolved.toColumn}`, "=",
		`${sourceRef}.${resolved.fromColumn}`,
	);

	// Extra ON conditions
	for (const cond of config.onConditions) {
		if (cond.kind === "ref") {
			inner = inner.whereRef(`${targetTable}.${cond.lhs}`, cond.op, `${sourceRef}.${cond.rhs}`);
		} else {
			inner = inner.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
		}
	}

	inner = inner.limit(1);

	return sql`(SELECT to_jsonb(sub) FROM (${inner}) sub)`.as(config.alias);
}

// ---------------------------------------------------------------------------
// CTE-based mutation building
// ---------------------------------------------------------------------------

/** A CTE definition to add to the query */
export interface CTEDef {
	name: string;
	query: any; // Kysely query (compiled with .compile())
}

/** How the main SELECT references a mutation CTE */
export interface MutationRef {
	alias: string;        // Output column alias
	cteName: string;      // CTE (or assembly CTE) to reference
	resolved: ResolvedRelation;
	fireAndForget: boolean;
	hasProjection: boolean; // Whether a projection narrows the RETURNING columns
	stripFkFromJson: boolean; // Whether to strip the FK from JSON (only if projection excludes it)
}

/**
 * Build a scope subquery: SELECT fromColumn FROM table WHERE <conditions>
 * This is used to correlate mutation CTEs to the parent query.
 */
export function buildScopeSubquery(
	db: Kysely<any>, table: string, fromColumn: string, wheres: any[][],
) {
	let scope = db.selectFrom(table).select(`${table}.${fromColumn}` as any);
	for (const w of wheres) {
		scope = scope.where(...w as [any, any, any]);
	}
	return scope;
}

/**
 * Build a scope subquery for through-table relations.
 * SELECT through.to FROM through WHERE through.from IN (parentScope)
 */
function buildThroughScope(
	db: Kysely<any>,
	through: { table: string; from: string; to: string },
	parentScope: any,
) {
	return db.selectFrom(through.table)
		.select(`${through.table}.${through.to}` as any)
		.where(`${through.table}.${through.from}`, "in", parentScope);
}

/** Counter for unique CTE names */
let cteCounter = 0;

/** Reset CTE counter (call before building a query) */
export function resetCTECounter() {
	cteCounter = 0;
}

function nextCteName(prefix: string, alias: string): string {
	return `_${prefix}_${alias}_${cteCounter++}`;
}

/**
 * Recursively build mutation CTEs for all mutation relations.
 *
 * @param db Kysely instance
 * @param meta Database metadata
 * @param parentTable The parent table (source of the relation)
 * @param parentWheres WHERE conditions on the parent query (for scope building)
 * @param relations Relations to process
 * @param parentScopeBase Optional pre-built scope base for nested mutations.
 *   This is a `selectFrom(parentTable).where(...)` without `.select()`,
 *   so each child relation can add its own `fromColumn` via `.select()`.
 * @returns CTEs to prepend and references for the main SELECT
 */
export function buildMutationCTEs(
	db: Kysely<any>,
	meta: AnyMeta,
	parentTable: string,
	parentWheres: any[][],
	relations: RelationRequest[],
	parentScopeBase?: any,
): { ctes: CTEDef[]; refs: MutationRef[] } {
	const ctes: CTEDef[] = [];
	const refs: MutationRef[] = [];

	for (const rel of relations) {
		if (!rel.mutation) continue;

		const { config, resolved, mutation, fireAndForget } = rel;
		const targetTable = resolved.targetTable;

		// Build the scope subquery for this relation's correlation.
		// parentScopeBase is a selectFrom(parentTable).where(...) without .select().
		// We add .select(fromColumn) to get the scope for this specific relation.
		const scope = parentScopeBase
			? parentScopeBase.select(`${parentTable}.${resolved.fromColumn}` as any)
			: buildScopeSubquery(db, parentTable, resolved.fromColumn, parentWheres);

		const correlationScope = resolved.through
			? buildThroughScope(db, resolved.through, scope)
			: scope;

		// Recursively build nested mutation CTEs first (deepest-first)
		let nestedCtes: CTEDef[] = [];
		let nestedMutRefs: MutationRef[] = [];
		let nestedReadRels: RelationRequest[] = [];
		if (mutation.nested.length > 0) {
			const nestedRelations = resolveNestedRefs(db, meta, targetTable, mutation.nested);

			// Separate into mutations and reads
			const nestedMutations = nestedRelations.filter(r => r.mutation);
			nestedReadRels = nestedRelations.filter(r => !r.mutation && !r.fireAndForget);

			if (nestedMutations.length > 0) {
				// Build a scope base for the target table, filtered by the current correlation.
				// Nested mutations will chain through this: SELECT target.childFromCol FROM target WHERE target.toCol IN (scope)
				const nestedScopeBase = db.selectFrom(targetTable)
					.where(`${targetTable}.${resolved.toColumn}`, "in", correlationScope);
				const nestedResult = buildMutationCTEs(
					db, meta, targetTable, [],
					nestedMutations, nestedScopeBase,
				);
				nestedCtes = nestedResult.ctes;
				nestedMutRefs = nestedResult.refs;
			}
		}

		// Add nested CTEs first
		ctes.push(...nestedCtes);

		// Build the mutation CTE
		const cteName = nextCteName(mutation.kind === "update" ? "upd" : mutation.kind === "delete" ? "del" : "ins", config.alias);

		const mutQuery = buildMutationQuery(
			db, meta, targetTable, mutation, resolved, config,
			correlationScope,
		);
		ctes.push({ name: cteName, query: mutQuery });

		// Determine if we should strip the FK from JSON results:
		// Only strip if there's a projection AND the FK was not originally in it.
		const stripFkFromJson = mutation.projection
			? !(meta[targetTable]?.projections?.[mutation.projection] as readonly string[] | undefined)?.includes(resolved.toColumn)
			: false;

		// If the mutation has nested children (mutation refs or read relations),
		// build an assembly CTE that combines mutation RETURNING with nested data
		const dataMutRefs = nestedMutRefs.filter(r => !r.fireAndForget);
		if ((dataMutRefs.length > 0 || nestedReadRels.length > 0) && !fireAndForget) {
			const aggName = nextCteName("agg", config.alias);
			const aggQuery = buildAssemblyCTE(db, meta, cteName, targetTable, mutation.projection, dataMutRefs, nestedReadRels);
			ctes.push({ name: aggName, query: aggQuery });
			refs.push({ alias: config.alias, cteName: aggName, resolved, fireAndForget: !!fireAndForget, hasProjection: !!mutation.projection, stripFkFromJson });
		} else {
			refs.push({ alias: config.alias, cteName, resolved, fireAndForget: !!fireAndForget, hasProjection: !!mutation.projection, stripFkFromJson });
		}
	}

	return { ctes, refs };
}

/**
 * Build the actual mutation query (UPDATE/DELETE/INSERT) for a CTE.
 */
function buildMutationQuery(
	db: Kysely<any>,
	meta: AnyMeta,
	targetTable: string,
	mutation: RelationMutation,
	resolved: ResolvedRelation,
	config: RelBuilderConfig,
	correlationScope: any,
) {
	if (mutation.kind === "update") {
		let q = db.updateTable(targetTable).set(mutation.set!);
		q = q.where(`${targetTable}.${resolved.toColumn}`, "in", correlationScope);
		for (const cond of config.onConditions) {
			if (cond.kind === "ref") {
				// For CTE-based, ref conditions can't reference the source row.
				// We use the target table's column with a value condition instead.
				q = q.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
			} else {
				q = q.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
			}
		}
		for (const w of mutation.wheres) {
			q = q.where(...w as [any, any, any]);
		}
		if (mutation.projection) {
			const tableMeta = meta[targetTable];
			const columns = tableMeta?.projections?.[mutation.projection];
			if (columns) {
				// IMPORTANT: Always include the correlation column (toColumn) in RETURNING,
				// even if it's not in the projection, so the CTE can be correlated in the outer query.
				const returnCols = new Set([...columns, resolved.toColumn]);
				return q.returning([...returnCols].map((c) => `${targetTable}.${c}` as any));
			}
		}
		return q.returningAll();
	}

	if (mutation.kind === "delete") {
		let q = db.deleteFrom(targetTable);
		q = q.where(`${targetTable}.${resolved.toColumn}`, "in", correlationScope);
		for (const cond of config.onConditions) {
			if (cond.kind === "ref") {
				q = q.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
			} else {
				q = q.where(`${targetTable}.${cond.lhs}`, cond.op, cond.rhs);
			}
		}
		for (const w of mutation.wheres) {
			q = q.where(...w as [any, any, any]);
		}
		return q.returning(sql`1`.as("_"));
	}

	// INSERT
	// Auto-inject the FK column from the correlation scope.
	// The user should not provide the FK in values — it's set automatically.
	const fkCol = resolved.toColumn;
	const stripFK = (v: any) => { const { [fkCol]: _, ...rest } = v; return rest; };
	const addFK = (v: any) => ({ ...v, [fkCol]: sql`(${correlationScope})` });
	const rawValues = mutation.values!;
	const augmentedValues = Array.isArray(rawValues)
		? rawValues.map((v: any) => addFK(stripFK(v)))
		: addFK(stripFK(rawValues));
	let q = db.insertInto(targetTable).values(augmentedValues);
	if (mutation.onConflict) {
		q = q.onConflict(mutation.onConflict);
	}
	if (mutation.projection) {
		const tableMeta = meta[targetTable];
		const columns = tableMeta?.projections?.[mutation.projection];
		if (columns) {
			// IMPORTANT: Always include the correlation column (toColumn) in RETURNING,
			// even if it's not in the projection, so the CTE can be correlated in the outer query.
			const returnCols = new Set([...columns, resolved.toColumn]);
			return q.returning([...returnCols].map((c) => `${targetTable}.${c}` as any));
		}
	}
	return q.returningAll();
}

/**
 * Build an assembly CTE that combines a mutation's RETURNING rows with nested relation data.
 *
 * SELECT s.*,
 *   COALESCE((SELECT jsonb_agg(to_jsonb(p)) FROM _mut_products p WHERE p.seller_id = s.id), '[]'::jsonb) AS products
 * FROM _mut_sellers s
 */
function buildAssemblyCTE(
	db: Kysely<any>,
	meta: AnyMeta,
	parentCteName: string,
	_parentTable: string,
	_parentProjection: string | undefined,
	nestedMutRefs: MutationRef[],
	nestedReadRels: RelationRequest[] = [],
) {
	let q = db.selectFrom(`${parentCteName} as _p`).selectAll("_p" as any);

	// Add mutation CTE references (nested mutations that return data)
	for (const ref of nestedMutRefs) {
		if (ref.fireAndForget) continue;

		const targetRel = ref.resolved;
		const jsonExpr = ref.stripFkFromJson
			? sql`to_jsonb(_c) - ${sql.lit(targetRel.toColumn)}`
			: sql`to_jsonb(_c)`;
		if (targetRel.type === "many") {
			const subq = sql`COALESCE((SELECT jsonb_agg(${jsonExpr}) FROM ${sql.ref(ref.cteName)} _c WHERE ${sql.ref(`_c.${targetRel.toColumn}`)} = ${sql.ref(`_p.${targetRel.fromColumn}`)}), '[]'::jsonb)`;
			q = q.select(subq.as(ref.alias)) as any;
		} else {
			const subq = sql`(SELECT ${jsonExpr} FROM ${sql.ref(ref.cteName)} _c WHERE ${sql.ref(`_c.${targetRel.toColumn}`)} = ${sql.ref(`_p.${targetRel.fromColumn}`)} LIMIT 1)`;
			q = q.select(subq.as(ref.alias)) as any;
		}
	}

	// Add read relation subqueries (nested reads referencing the mutation CTE rows)
	for (const req of nestedReadRels) {
		if (req.resolved.type === "many") {
			const innerQuery = buildToManyInnerQuery(db, meta, "_p", req);
			q = q.select(
				sql`COALESCE((SELECT jsonb_agg(to_jsonb(_x)) FROM (${innerQuery}) _x), '[]'::jsonb)`.as(req.config.alias),
			) as any;
		} else {
			q = q.select(buildToOneSubquery(db, meta, "_p", req)) as any;
		}
	}

	return q;
}

/**
 * Resolve nested relation references (from mutation builder's nested list)
 * into proper RelationRequests.
 */
function resolveNestedRefs(
	db: Kysely<any>,
	meta: AnyMeta,
	parentTable: string,
	nested: NestedRelationRef[],
): RelationRequest[] {
	const requests: RelationRequest[] = [];

	for (const ref of nested) {
		const request = resolveWithRelatedArgs(meta, parentTable, ref.nameOrBuilder as any);
		if (ref.variant) request.variant = ref.variant;
		request.fireAndForget = ref.fireAndForget;

		if (ref.modifier) {
			// Call the modifier with a probe builder to detect mutation vs read
			const probeQb = new OrmSelectQueryBuilder(
				db, meta as any, request.resolved.targetTable as any,
				db.selectFrom(request.resolved.targetTable),
				[], false, ["relation", "default"],
			);
			const result = ref.modifier(probeQb);
			if (isRelationMutationBuilder(result)) {
				request.mutation = result._toMutation();
			} else {
				request.modifier = ref.modifier as any;
			}
		}

		requests.push(request);
	}

	return requests;
}

/**
 * Apply mutation CTEs to a Kysely query using raw SQL.
 * Returns raw SQL that wraps the base query with WITH clauses.
 */
export function wrapQueryWithCTEs(
	baseQuery: any,
	ctes: CTEDef[],
) {
	if (ctes.length === 0) return baseQuery;

	// Use Kysely's .with() for each CTE
	let q = baseQuery;
	for (const cte of ctes) {
		q = q.with(cte.name, () => cte.query);
	}
	return q;
}

/**
 * Build a scalar subquery referencing a mutation CTE for the main SELECT.
 * For toMany: COALESCE((SELECT jsonb_agg(to_jsonb(c)) FROM cte c WHERE c.toCol = parent.fromCol), '[]'::jsonb) AS alias
 * For toOne: (SELECT to_jsonb(c) FROM cte c WHERE c.toCol = parent.fromCol LIMIT 1) AS alias
 */
export function buildMutationCTERef(
	parentRef: string,
	ref: MutationRef,
) {
	const { cteName, alias, resolved } = ref;
	// When a projection excludes the FK, strip it from JSON results.
	// If the projection explicitly includes the FK, keep it in the result.
	const jsonExpr = ref.stripFkFromJson
		? sql`to_jsonb(_r) - ${sql.lit(resolved.toColumn)}`
		: sql`to_jsonb(_r)`;
	if (resolved.type === "many") {
		return sql`COALESCE((SELECT jsonb_agg(${jsonExpr}) FROM ${sql.table(cteName)} _r WHERE ${sql.ref(`_r.${resolved.toColumn}`)} = ${sql.ref(`${parentRef}.${resolved.fromColumn}`)}), '[]'::jsonb)`.as(alias);
	} else {
		return sql`(SELECT ${jsonExpr} FROM ${sql.table(cteName)} _r WHERE ${sql.ref(`_r.${resolved.toColumn}`)} = ${sql.ref(`${parentRef}.${resolved.fromColumn}`)} LIMIT 1)`.as(alias);
	}
}
