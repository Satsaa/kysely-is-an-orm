import { Kysely } from "kysely";
import { type MetaDB } from "./meta.js";
import { OrmSelectQueryBuilder } from "./select-builder.js";
import { OrmInsertQueryBuilder, OrmUpdateQueryBuilder, OrmDeleteQueryBuilder } from "./mutation-builders.js";

// ---------------------------------------------------------------------------
// OrmKysely — enhanced Kysely with relation support on all operations
// ---------------------------------------------------------------------------

export interface OrmKysely<
	DB extends Record<string, any>,
	M extends MetaDB<DB>,
> extends Omit<Kysely<DB>, "selectFrom" | "insertInto" | "updateTable" | "deleteFrom"> {
	selectFrom<T extends keyof DB & string>(table: T): OrmSelectQueryBuilder<DB, T, {}, M, false>;
	insertInto<T extends keyof DB & string>(table: T): OrmInsertQueryBuilder<DB, T, M>;
	updateTable<T extends keyof DB & string>(table: T): OrmUpdateQueryBuilder<DB, T, M>;
	deleteFrom<T extends keyof DB & string>(table: T): OrmDeleteQueryBuilder<DB, T, M>;
}

export function createOrm<
	DB extends Record<string, any>,
	M extends MetaDB<DB>,
>(db: Kysely<DB>, meta: M): OrmKysely<DB, M> {
	return new Proxy(db as any, {
		get(target, prop) {
			if (prop === "selectFrom") {
				return <T extends keyof DB & string>(table: T) => {
					const inner = target.selectFrom(table);
					return new OrmSelectQueryBuilder(db, meta, table, inner as any, []);
				};
			}
			if (prop === "insertInto") {
				return <T extends keyof DB & string>(table: T) => {
					const inner = target.insertInto(table);
					return new OrmInsertQueryBuilder(db, meta, table, inner);
				};
			}
			if (prop === "updateTable") {
				return <T extends keyof DB & string>(table: T) => {
					const inner = target.updateTable(table);
					return new OrmUpdateQueryBuilder(db, meta, table, inner);
				};
			}
			if (prop === "deleteFrom") {
				return <T extends keyof DB & string>(table: T) => {
					const inner = target.deleteFrom(table);
					return new OrmDeleteQueryBuilder(db, meta, table, inner);
				};
			}
			const value = target[prop];
			return typeof value === "function" ? value.bind(target) : value;
		},
	}) as OrmKysely<DB, M>;
}
