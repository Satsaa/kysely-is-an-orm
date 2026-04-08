/**
 * Centralized database metadata definition.
 *
 * Instead of defining models with circular references, define all table
 * metadata in one place. Relations reference tables by name (string),
 * avoiding circular type issues entirely.
 */

// ---------------------------------------------------------------------------
// Core types for metadata definition
// ---------------------------------------------------------------------------

/**
 * A relation definition. TypeScript infers `to` column types from `model`.
 * `through.from` and `through.to` are typed as columns of the through table.
 */
export type RelationDef<
	DB,
	FromTable extends keyof DB,
	ToTable extends keyof DB = keyof DB,
	ThroughTable extends keyof DB = keyof DB,
> = {
	model: ToTable;
	type: "one" | "many";
	from: keyof DB[FromTable] & string;
	to: keyof DB[ToTable] & string;
	through?: {
		[TT in ThroughTable]: {
			table: TT & string;
			from: keyof DB[TT] & string;
			to: keyof DB[TT] & string;
		};
	}[ThroughTable];
};

/**
 * Union of all valid relation shapes for a table.
 * When you set `model: "sellers"`, TypeScript narrows `to` to columns of `sellers`.
 */
export type AnyRelation<DB, FromTable extends keyof DB> = {
	[ToTable in keyof DB]: RelationDef<DB, FromTable, ToTable>;
}[keyof DB];

/**
 * Relations map for a table.
 */
export type Relations<DB, Table extends keyof DB> = {
	[name: string]: AnyRelation<DB, Table>;
};

/**
 * Metadata for a single table.
 */
export type TableMeta<DB, Table extends keyof DB> = {
	id?: keyof DB[Table] & string;
	projections?: {
		[name: string]: ReadonlyArray<keyof DB[Table] & string>;
	};
	relations?: Relations<DB, Table>;
};

/**
 * Full database metadata. Use with `satisfies` for type checking:
 *
 *   const meta = { ... } satisfies MetaDB<Database>
 */
export type MetaDB<DB extends Record<string, any>> = {
	[Table in keyof DB]?: TableMeta<DB, Table>;
};

// ---------------------------------------------------------------------------
// Runtime relation shape (used internally by query builder)
// ---------------------------------------------------------------------------

export interface ResolvedRelation {
	targetTable: string;
	type: "one" | "many";
	fromColumn: string;
	toColumn: string;
	through?: {
		table: string;
		from: string;
		to: string;
	};
}

/**
 * Resolves a relation definition to its runtime shape.
 */
export function resolveRelation<DB>(
	rel: AnyRelation<DB, keyof DB>,
): ResolvedRelation {
	return {
		targetTable: rel.model as string,
		type: rel.type,
		fromColumn: rel.from,
		toColumn: rel.to,
		through: rel.through
			? {
					table: rel.through.table as string,
					from: rel.through.from as string,
					to: rel.through.to as string,
				}
			: undefined,
	};
}
