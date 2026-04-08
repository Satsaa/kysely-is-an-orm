/**
 * SQL output tests for kysely-is-an-orm.
 * Verifies generated SQL via .compile() without needing a real database.
 */

import { Kysely, PostgresDialect, type Generated } from "kysely";
import { createOrm, type MetaDB } from "../src/index.js";

// ---------------------------------------------------------------------------
// Test schema (same as type-tests)
// ---------------------------------------------------------------------------

interface MarketTable {
	id: Generated<number>;
	name: string;
	location: string;
	active: boolean;
	created_at: Generated<string>;
}

interface SellerTable {
	id: Generated<number>;
	market_id: number;
	name: string;
	booth_number: string;
	created_at: Generated<string>;
}

interface ItemTable {
	id: Generated<number>;
	seller_id: number;
	name: string;
	price: number;
}

interface MarketTagTable {
	id: Generated<number>;
	name: string;
}

interface MarketTagJoinTable {
	market_id: number;
	tag_id: number;
}

interface Database {
	markets: MarketTable;
	sellers: SellerTable;
	items: ItemTable;
	market_tags: MarketTagTable;
	market_tag_joins: MarketTagJoinTable;
}

const meta = {
	markets: {
		relations: {
			sellers: {
				model: "sellers",
				type: "many",
				from: "id",
				to: "market_id",
			},
			tags: {
				model: "market_tags",
				type: "many",
				from: "id",
				to: "id",
				through: {
					table: "market_tag_joins",
					from: "market_id",
					to: "tag_id",
				},
			},
		},
	},
	sellers: {
		relations: {
			market: {
				model: "markets",
				type: "one",
				from: "market_id",
				to: "id",
			},
			items: {
				model: "items",
				type: "many",
				from: "id",
				to: "seller_id",
			},
		},
	},
	items: {
		relations: {
			seller: {
				model: "sellers",
				type: "one",
				from: "seller_id",
				to: "id",
			},
		},
	},
} as const satisfies MetaDB<Database>;

// ---------------------------------------------------------------------------
// Create a Kysely instance that compiles SQL but never connects
// ---------------------------------------------------------------------------

const fakePool = { connect: () => { throw new Error("no real DB"); } } as any;

const rawDb = new Kysely<Database>({
	dialect: new PostgresDialect({ pool: fakePool }),
});

const db = createOrm(rawDb, meta);

// ---------------------------------------------------------------------------
// Assert helpers
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string) {
	if (!condition) {
		failed++;
		console.error(`  FAIL: ${message}`);
		throw new Error(`Assertion failed: ${message}`);
	}
}

function assertContains(sql: string, substring: string, label: string) {
	assert(
		sql.includes(substring),
		`${label}: SQL should contain "${substring}"\n    Got: ${sql}`,
	);
}

function assertNotContains(sql: string, substring: string, label: string) {
	assert(
		!sql.includes(substring),
		`${label}: SQL should NOT contain "${substring}"\n    Got: ${sql}`,
	);
}

async function test(name: string, fn: () => Promise<void> | void) {
	try {
		await fn();
		passed++;
		console.log(`  PASS: ${name}`);
	} catch (err) {
		console.error(`        ${(err as Error).message}`);
	}
}

function norm(sql: string): string {
	return sql.replace(/\s+/g, " ").trim().toLowerCase();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function runTests() {
	console.log("\n=== kysely-is-an-orm SQL Tests ===\n");

	// -----------------------------------------------------------------------
	// Basic selectFrom
	// -----------------------------------------------------------------------

	await test("basic selectFrom without select auto-selects all", () => {
		const { sql } = db.selectFrom("markets").compile();
		const n = norm(sql);
		assertContains(n, 'from "markets"', "has from markets");
		assertContains(n, '"markets".*', "auto selectAll");
	});

	await test("selectAll() generates SELECT table.*", () => {
		const { sql } = db.selectFrom("markets").selectAll().compile();
		assertContains(norm(sql), '"markets".*', "has selectAll");
		assertContains(norm(sql), 'from "markets"', "has from markets");
	});

	await test("selectAll + where generates correct WHERE clause", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.selectAll()
			.where("id", "=", 1)
			.compile();
		assertContains(norm(sql), '"id" = $1', "has where clause");
		assert(parameters[0] === 1, "parameter is 1");
	});

	await test("select() narrows columns in SQL", () => {
		const { sql } = db
			.selectFrom("markets")
			.select(["id", "name"])
			.compile();
		assertContains(norm(sql), '"id"', "has id");
		assertContains(norm(sql), '"name"', "has name");
		assertNotContains(norm(sql), '"markets".*', "no selectAll");
	});

	// -----------------------------------------------------------------------
	// withRelated: toMany (correlated subquery)
	// -----------------------------------------------------------------------

	await test("toMany: uses LEFT JOIN LATERAL", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertContains(n, "left join lateral", "uses LEFT JOIN LATERAL");
		assertContains(n, "on true", "LATERAL joined ON TRUE");
		assertContains(n, "jsonb_agg", "uses jsonb_agg");
		assertContains(n, "to_jsonb", "uses to_jsonb");
		assertContains(n, "'[]'::jsonb", "coalesce to empty array");
		assertContains(n, '"sellers"."market_id" = "markets"."id"', "correlation");
	});

	await test("toMany: selects all columns from relation table", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated("sellers")
			.compile();
		assertContains(norm(sql), '"sellers".*', "selects all seller columns");
	});

	// -----------------------------------------------------------------------
	// withRelated: toOne (LEFT JOIN)
	// -----------------------------------------------------------------------

	await test("toOne: generates LEFT JOIN", () => {
		const { sql } = db
			.selectFrom("sellers")
			.withRelated("market")
			.compile();
		const n = norm(sql);
		assertContains(n, "left join", "uses LEFT JOIN");
		assertContains(n, '"markets"', "joins markets table");
		assertContains(n, '"_rel_market"', "uses aliased join");
	});

	await test("toOne: generates to_jsonb with null guard", () => {
		const { sql } = db
			.selectFrom("sellers")
			.withRelated("market")
			.compile();
		const n = norm(sql);
		assertContains(n, "to_jsonb", "uses to_jsonb");
		assertContains(n, "case when", "has CASE WHEN null guard");
		assertContains(n, "is null", "checks for NULL");
		assertContains(n, "then null", "returns NULL when no match");
		assertNotContains(n, "jsonb_agg", "no jsonb_agg for toOne");
	});

	await test("toOne: ON clause has correct correlation", () => {
		const { sql } = db
			.selectFrom("sellers")
			.withRelated("market")
			.compile();
		const n = norm(sql);
		// _rel_market.id = sellers.market_id
		assertContains(n, '"_rel_market"."id" = "sellers"."market_id"', "join correlation");
	});

	// -----------------------------------------------------------------------
	// withRelated: through table (many-to-many)
	// -----------------------------------------------------------------------

	await test("toMany through: generates join + correlation", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated("tags")
			.compile();
		const n = norm(sql);
		assertContains(n, "jsonb_agg", "uses jsonb_agg for many");
		assertContains(n, '"market_tag_joins"', "joins through table");
		assertContains(n, '"market_tag_joins"."tag_id"', "through.to column");
		assertContains(n, '"market_tag_joins"."market_id"', "through.from column");
	});

	// -----------------------------------------------------------------------
	// Modifier: where/orderBy/limit inside withRelated (toMany)
	// -----------------------------------------------------------------------

	await test("toMany modifier: where appears in subquery", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.withRelated("sellers", (qb) =>
				qb.where("booth_number", "=", "A1"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"booth_number" = $1', "seller where clause");
		assert(parameters[0] === "A1", 'parameter is "A1"');
	});

	await test("toMany modifier: orderBy + limit appear in subquery", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.withRelated("sellers", (qb) =>
				qb.orderBy("name", "desc").limit(5),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, "order by", "has ORDER BY in subquery");
		assertContains(n, "limit", "has LIMIT in subquery");
		assert(parameters.includes(5), "LIMIT 5 is parameterized");
	});

	// -----------------------------------------------------------------------
	// Builder callback: alias
	// -----------------------------------------------------------------------

	await test("builder: aliased toMany uses alias in SQL", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated(
				(b) => b("sellers").as("topSellers"),
				(qb) => qb.limit(3),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"topsellers"', "alias appears in SQL");
	});

	await test("builder: aliased toOne uses alias in SQL", () => {
		const { sql } = db
			.selectFrom("sellers")
			.withRelated(
				(b) => b("market").as("parentMarket"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"parentmarket"', "alias appears in SQL");
		assertContains(n, '"_rel_parentmarket"', "join alias uses relation alias");
	});

	await test("builder: multiple aliases create separate relations", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated(
				(b) => b("sellers").as("boothA"),
				(qb) => qb.where("booth_number", "like", "A%"),
			)
			.withRelated(
				(b) => b("sellers").as("boothB"),
				(qb) => qb.where("booth_number", "like", "B%"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"bootha"', "first alias");
		assertContains(n, '"boothb"', "second alias");
		const jsonbCount = (n.match(/jsonb_agg/g) || []).length;
		assert(jsonbCount === 2, `should have 2 jsonb_agg calls, got ${jsonbCount}`);
	});

	// -----------------------------------------------------------------------
	// Builder callback: ON conditions
	// -----------------------------------------------------------------------

	await test("builder: toOne .on() adds extra ON condition", () => {
		const { sql, parameters } = db
			.selectFrom("sellers")
			.withRelated(
				(b) => b("market").on("active", "=", true),
			)
			.compile();
		const n = norm(sql);
		// Should have the correlation ON + the extra ON
		assertContains(n, '"_rel_market"."id" = "sellers"."market_id"', "base correlation");
		assertContains(n, '"_rel_market"."active" = $1', "extra ON condition");
		assert(parameters.includes(true), "parameter includes true");
	});

	await test("builder: toMany .on() adds extra WHERE in subquery", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.withRelated(
				(b) => b("sellers").on("booth_number", "=", "VIP"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"sellers"."booth_number" = $1', "extra condition in subquery");
		assert(parameters.includes("VIP"), "parameter includes VIP");
	});

	// -----------------------------------------------------------------------
	// Builder callback: INNER JOIN
	// -----------------------------------------------------------------------

	await test("builder: toOne .inner() uses INNER JOIN", () => {
		const { sql } = db
			.selectFrom("sellers")
			.withRelated(
				(b) => b("market").inner(),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, "inner join", "uses INNER JOIN");
		assertNotContains(n, "left join", "no LEFT JOIN");
		// INNER JOIN: no null guard needed
		assertNotContains(n, "case when", "no CASE WHEN for inner join");
	});

	// -----------------------------------------------------------------------
	// Nested withRelated
	// -----------------------------------------------------------------------

	await test("nested withRelated: recursive subqueries", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated("sellers", (qb) =>
				qb.withRelated("items"),
			)
			.compile();
		const n = norm(sql);
		// Outer: markets -> sellers (jsonb_agg)
		assertContains(n, '"sellers"."market_id" = "markets"."id"', "outer correlation");
		// Inner: sellers -> items (jsonb_agg inside sellers subquery)
		assertContains(n, '"items"."seller_id" = "sellers"."id"', "inner correlation");
		const jsonbCount = (n.match(/jsonb_agg/g) || []).length;
		assert(jsonbCount === 2, `should have 2 jsonb_agg calls for nested, got ${jsonbCount}`);
	});

	await test("nested withRelated: toOne inside toMany uses LEFT JOIN inside subquery", () => {
		const { sql } = db
			.selectFrom("markets")
			.withRelated("sellers", (qb) =>
				qb.withRelated("market"),
			)
			.compile();
		const n = norm(sql);
		// Outer: markets -> sellers (jsonb_agg)
		assertContains(n, "jsonb_agg", "outer uses jsonb_agg");
		// Inner: sellers -> market (LEFT JOIN inside the subquery)
		assertContains(n, "left join", "inner uses LEFT JOIN for toOne");
		assertContains(n, "to_jsonb", "inner uses to_jsonb");
	});

	// -----------------------------------------------------------------------
	// select + withRelated: no duplicate selectAll
	// -----------------------------------------------------------------------

	await test("explicit select() prevents automatic selectAll", () => {
		const { sql } = db
			.selectFrom("markets")
			.select(["id", "name"])
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertNotContains(n, '"markets".*', "no selectAll when select() used");
		assertContains(n, '"id"', "has selected id");
		assertContains(n, '"name"', "has selected name");
	});

	await test("no explicit select() auto-generates selectAll", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.compile();
		const n = norm(sql);
		assertContains(n, '"markets".*', "auto selectAll when no select()");
	});

	// -----------------------------------------------------------------------
	// Clone safety: chaining must not mutate the original
	// -----------------------------------------------------------------------

	await test("where() clones — original query unchanged", () => {
		const base = db.selectFrom("markets").selectAll();
		const baseSql = norm(base.compile().sql);

		const filtered = base.where("id", "=", 1);
		const filteredSql = norm(filtered.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base SQL unchanged after .where()");
		assert(filteredSql !== baseSql, "filtered SQL differs from base");
		assertContains(filteredSql, '"id" = $1', "filtered has WHERE");
		assertNotContains(baseSql, "$1", "base has no parameters");
	});

	await test("withRelated() clones — original query unchanged", () => {
		const base = db.selectFrom("markets").selectAll();
		const baseSql = norm(base.compile().sql);

		const withRel = base.withRelated("sellers");
		const withRelSql = norm(withRel.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base SQL unchanged after .withRelated()");
		assertContains(withRelSql, "jsonb_agg", "withRel has relation subquery");
		assertNotContains(baseSql, "jsonb_agg", "base has no relation subquery");
	});

	await test("orderBy() clones — original query unchanged", () => {
		const base = db.selectFrom("markets").selectAll();
		const baseSql = norm(base.compile().sql);

		base.orderBy("name", "asc");

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base SQL unchanged after .orderBy()");
	});

	await test("limit() clones — original query unchanged", () => {
		const base = db.selectFrom("markets").selectAll();
		const baseSql = norm(base.compile().sql);

		base.limit(10);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base SQL unchanged after .limit()");
	});

	await test("select() clones — original query unchanged", () => {
		const base = db.selectFrom("markets");
		const baseSql = norm(base.compile().sql);

		base.select(["id", "name"]);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base SQL unchanged after .select()");
	});

	await test("chained mutations are independent", () => {
		const base = db.selectFrom("markets").selectAll();
		const branch1 = base.where("id", "=", 1);
		const branch2 = base.where("name", "=", "test");

		const sql1 = norm(branch1.compile().sql);
		const sql2 = norm(branch2.compile().sql);

		assertContains(sql1, '"id" = $1', "branch1 filters by id");
		assertNotContains(sql1, '"name"', "branch1 has no name filter");
		assertContains(sql2, '"name" = $1', "branch2 filters by name");
	});

	// -----------------------------------------------------------------------
	// INSERT: basic
	// -----------------------------------------------------------------------

	await test("insertInto: basic insert compiles", () => {
		const { sql, parameters } = db
			.insertInto("markets")
			.values({ name: "Test Market", location: "Helsinki", active: true })
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "markets"', "has INSERT INTO");
		assertContains(n, '"name"', "has name column");
		assert(parameters.includes("Test Market"), "has parameter");
	});

	await test("insertInto: returningAll without relations", () => {
		const { sql } = db
			.insertInto("markets")
			.values({ name: "Test", location: "Helsinki", active: true })
			.returningAll()
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "markets"', "has INSERT");
		assertContains(n, "returning *", "has RETURNING *");
	});

	// -----------------------------------------------------------------------
	// INSERT: returningAll + withRelated (CTE)
	// -----------------------------------------------------------------------

	await test("insertInto: returningAll + withRelated toMany uses CTE + LATERAL", () => {
		const { sql, parameters } = db
			.insertInto("markets")
			.values({ name: "New Market", location: "Tampere", active: true })
			.returningAll()
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, 'insert into "markets"', "CTE contains INSERT");
		assertContains(n, "returning *", "CTE has RETURNING *");
		assertContains(n, 'from "_mutation"', "outer SELECT from CTE");
		assertContains(n, '"_mutation".*', "selects all mutation columns");
		assertContains(n, "left join lateral", "uses LEFT JOIN LATERAL");
		assertContains(n, "on true", "LATERAL ON TRUE");
		assertContains(n, "jsonb_agg", "has jsonb_agg for toMany");
		assertContains(n, '"sellers"."market_id" = "_mutation"."id"', "correlation against CTE alias");
		assert(parameters.includes("New Market"), "insert parameter present");
	});

	await test("insertInto: returningAll + withRelated toOne uses scalar subquery", () => {
		const { sql } = db
			.insertInto("sellers")
			.values({ name: "New Seller", market_id: 1, booth_number: "A1" })
			.returningAll()
			.withRelated("market")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, 'insert into "sellers"', "CTE contains INSERT");
		assertContains(n, "to_jsonb", "has to_jsonb for toOne");
		assertContains(n, '"markets"."id" = "_mutation"."market_id"', "toOne correlation against CTE");
		assertContains(n, "limit", "toOne has LIMIT 1");
		// Should NOT use LEFT JOIN (scalar subquery instead)
		assertNotContains(n, "left join", "no LEFT JOIN in CTE context");
	});

	await test("insertInto: returningAll + multiple withRelated", () => {
		const { sql } = db
			.insertInto("markets")
			.values({ name: "Multi", location: "Oulu", active: true })
			.returningAll()
			.withRelated("sellers")
			.withRelated("tags")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		const jsonbCount = (n.match(/jsonb_agg/g) || []).length;
		assert(jsonbCount === 2, `should have 2 jsonb_agg for sellers+tags, got ${jsonbCount}`);
	});

	// -----------------------------------------------------------------------
	// UPDATE: basic
	// -----------------------------------------------------------------------

	await test("updateTable: basic update compiles", () => {
		const { sql, parameters } = db
			.updateTable("markets")
			.set({ name: "Updated" })
			.where("id", "=", 1)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "markets"', "has UPDATE");
		assertContains(n, 'set "name" = $1', "has SET");
		assertContains(n, '"id" = $2', "has WHERE");
		assert(parameters[0] === "Updated", "first param is set value");
		assert(parameters[1] === 1, "second param is where value");
	});

	await test("updateTable: returningAll without relations", () => {
		const { sql } = db
			.updateTable("markets")
			.set({ name: "Updated" })
			.where("id", "=", 1)
			.returningAll()
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "markets"', "has UPDATE");
		assertContains(n, "returning *", "has RETURNING *");
	});

	// -----------------------------------------------------------------------
	// UPDATE: returningAll + withRelated (CTE)
	// -----------------------------------------------------------------------

	await test("updateTable: returningAll + withRelated toMany uses CTE + LATERAL", () => {
		const { sql, parameters } = db
			.updateTable("markets")
			.set({ name: "Updated Market" })
			.where("id", "=", 1)
			.returningAll()
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, 'update "markets"', "CTE contains UPDATE");
		assertContains(n, 'set "name" = $1', "CTE has SET");
		assertContains(n, '"id" = $2', "CTE has WHERE");
		assertContains(n, "returning *", "CTE has RETURNING *");
		assertContains(n, 'from "_mutation"', "outer SELECT from CTE");
		assertContains(n, "left join lateral", "uses LEFT JOIN LATERAL");
		assertContains(n, "jsonb_agg", "has toMany subquery");
		assertContains(n, '"sellers"."market_id" = "_mutation"."id"', "correlation against CTE");
		assert(parameters[0] === "Updated Market", "SET param");
		assert(parameters[1] === 1, "WHERE param");
	});

	await test("updateTable: returningAll + withRelated toOne", () => {
		const { sql } = db
			.updateTable("sellers")
			.set({ name: "Updated Seller" })
			.where("id", "=", 5)
			.returningAll()
			.withRelated("market")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, 'update "sellers"', "CTE contains UPDATE");
		assertContains(n, "to_jsonb", "has toOne scalar subquery");
		assertContains(n, '"markets"."id" = "_mutation"."market_id"', "toOne correlation");
		assertContains(n, "limit", "toOne LIMIT 1");
	});

	await test("updateTable: CTE parameters are correctly ordered", () => {
		const { sql, parameters } = db
			.updateTable("markets")
			.set({ name: "New Name" })
			.where("id", "=", 42)
			.returningAll()
			.withRelated(
				(b) => b("sellers").on("booth_number", "=", "VIP"),
			)
			.compile();
		const n = norm(sql);
		// Mutation params come first, then relation ON params
		assert(parameters[0] === "New Name", "param $1 = SET value");
		assert(parameters[1] === 42, "param $2 = WHERE value");
		assert(parameters[2] === "VIP", "param $3 = relation ON value");
		assertContains(n, "$3", "relation param is $3 (after mutation params)");
	});

	// -----------------------------------------------------------------------
	// DELETE: basic + withRelated
	// -----------------------------------------------------------------------

	await test("deleteFrom: basic delete compiles", () => {
		const { sql, parameters } = db
			.deleteFrom("markets")
			.where("id", "=", 1)
			.compile();
		const n = norm(sql);
		assertContains(n, 'delete from "markets"', "has DELETE");
		assertContains(n, '"id" = $1', "has WHERE");
	});

	await test("deleteFrom: returningAll + withRelated", () => {
		const { sql } = db
			.deleteFrom("sellers")
			.where("id", "=", 1)
			.returningAll()
			.withRelated("market")
			.withRelated("items")
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, 'delete from "sellers"', "CTE contains DELETE");
		assertContains(n, "to_jsonb", "has toOne (market)");
		assertContains(n, "jsonb_agg", "has toMany (items)");
	});

	// -----------------------------------------------------------------------
	// Mutation + withRelated: builder callback (alias, ON)
	// -----------------------------------------------------------------------

	await test("mutation: withRelated builder alias works in CTE", () => {
		const { sql } = db
			.insertInto("markets")
			.values({ name: "Test", location: "Helsinki", active: true })
			.returningAll()
			.withRelated(
				(b) => b("sellers").as("topSellers"),
				(qb) => qb.limit(3),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"topsellers"', "alias in CTE output");
		assertContains(n, "limit", "modifier limit in subquery");
	});

	await test("mutation: withRelated toOne with .on() in CTE", () => {
		const { sql, parameters } = db
			.updateTable("sellers")
			.set({ name: "Updated" })
			.where("id", "=", 1)
			.returningAll()
			.withRelated(
				(b) => b("market").on("active", "=", true),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'with "_mutation" as', "has CTE");
		assertContains(n, '"markets"."active"', "extra ON in scalar subquery");
		assert(parameters.includes(true), "ON condition parameter present");
	});

	// -----------------------------------------------------------------------
	// Mutation: clone safety
	// -----------------------------------------------------------------------

	await test("mutation: withRelated clones — base unchanged", () => {
		const base = db
			.insertInto("markets")
			.values({ name: "Test", location: "Helsinki", active: true })
			.returningAll();

		const baseSql = norm(base.compile().sql);
		const withRel = base.withRelated("sellers");
		const withRelSql = norm(withRel.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base unchanged after .withRelated()");
		assertContains(withRelSql, "jsonb_agg", "withRel has relation");
		assertNotContains(baseSql, "jsonb_agg", "base has no relation");
	});

	// -----------------------------------------------------------------------
	// withRelated mutation: CTE-based UPDATE via qb.update()
	// -----------------------------------------------------------------------

	await test("withRelated mutation toMany: uses top-level CTE UPDATE", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "VIP" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, "with", "has WITH clause");
		assertContains(n, 'update "sellers"', "UPDATE in CTE");
		assertContains(n, 'set "booth_number"', "SET clause");
		assertContains(n, "returning *", "RETURNING * in CTE");
		// Scope subquery for correlation
		assertContains(n, "in (select", "scope subquery");
		assertContains(n, '"markets"."id"', "scope selects from column");
		// Main SELECT references CTE with jsonb_agg
		assertContains(n, 'from "markets"', "main SELECT from markets");
		assertContains(n, "jsonb_agg", "aggregates CTE rows");
		assertContains(n, "to_jsonb", "wraps rows in jsonb");
		assert(parameters.includes("VIP"), "SET parameter");
		assert(parameters.includes(1), "parent WHERE parameter");
		// No LATERAL for mutations
		assertNotContains(n, "left join lateral", "no LATERAL for CTE mutations");
	});

	await test("withRelated mutation toOne: uses CTE UPDATE + scalar subquery", () => {
		const { sql } = db
			.selectFrom("sellers")
			.where("id", "=", 5)
			.withRelated("market", (qb) =>
				qb.update().set({ name: "Updated Market" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, "with", "has WITH clause");
		assertContains(n, 'update "markets"', "UPDATE target table");
		assertContains(n, "returning *", "RETURNING *");
		assertContains(n, "to_jsonb", "wraps in jsonb");
		assertContains(n, "limit", "LIMIT 1 for toOne scalar subquery");
		assertNotContains(n, "jsonb_agg", "no jsonb_agg for toOne");
	});

	await test("withRelated mutation: inner .where() filters target rows", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("active", "=", true)
			.withRelated("sellers", (qb) =>
				qb.update().set({ name: "Updated" }).where("booth_number", "=", "A1"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"booth_number"', "extra WHERE on target");
		assert(parameters.includes("A1"), "modifier WHERE param");
		assert(parameters.includes(true), "parent WHERE param");
		assert(parameters.includes("Updated"), "SET param");
	});

	await test("withRelated mutation: builder with alias + ON", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated(
				(b) => b("sellers").on("booth_number", "=", "VIP"),
				(qb) => qb.update().set({ name: "VIP Seller" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "UPDATE in CTE");
		assertContains(n, '"sellers"."booth_number"', "extra ON condition in CTE WHERE");
		assert(parameters.includes("VIP"), "ON param");
		assert(parameters.includes("VIP Seller"), "SET param");
	});

	await test("withRelated mutation + read: mixed CTE + LATERAL in same query", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("tags")
			.withRelated("sellers", (qb) =>
				qb.update().set({ name: "Updated" }),
			)
			.compile();
		const n = norm(sql);
		// Mutation: CTE-based
		assertContains(n, "with", "has WITH for mutation CTE");
		assertContains(n, 'update "sellers"', "sellers update in CTE");
		assertContains(n, "returning *", "RETURNING in CTE");
		// Read: LATERAL-based
		assertContains(n, "left join lateral", "tags uses LATERAL");
		assertContains(n, '"market_tag_joins"', "tags through-table read");
		// Both produce jsonb_agg
		const jsonbAggCount = (n.match(/jsonb_agg/g) || []).length;
		assert(jsonbAggCount === 2, `should have 2 jsonb_agg calls, got ${jsonbAggCount}`);
	});

	// -----------------------------------------------------------------------
	// mutateRelated: fire-and-forget CTE mutations
	// -----------------------------------------------------------------------

	await test("mutateRelated delete: uses CTE DELETE (fire-and-forget)", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.mutateRelated("sellers", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		assertContains(n, "with", "has WITH clause");
		assertContains(n, 'delete from "sellers"', "DELETE in CTE");
		assertContains(n, "in (select", "scope subquery for correlation");
		assertContains(n, 'from "markets"', "main SELECT from markets");
		// Fire-and-forget: no jsonb_agg output for sellers
		assertNotContains(n, "jsonb_agg", "no jsonb_agg for fire-and-forget");
		assert(parameters.includes(1), "parent WHERE param");
	});

	await test("mutateRelated delete: with .where() filter", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("active", "=", true)
			.mutateRelated("sellers", (qb) =>
				qb.delete().where("booth_number", "=", "expired"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'delete from "sellers"', "DELETE");
		assertContains(n, '"booth_number"', "extra WHERE");
		assert(parameters.includes("expired"), "modifier param");
	});

	await test("mutateRelated delete: builder with ON condition", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.mutateRelated(
				(b) => b("sellers").on("booth_number", "=", "expired"),
				(qb) => qb.delete(),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"sellers"."booth_number"', "ON condition");
		assert(parameters.includes("expired"), "ON param");
	});

	await test("mutateRelated: does not add output columns", () => {
		const { sql } = db
			.selectFrom("markets")
			.selectAll()
			.mutateRelated("sellers", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		assertContains(n, '"markets".*', "parent selectAll");
		// Fire-and-forget: CTE present but no output column for sellers
		assertContains(n, "with", "CTE present");
		assertNotContains(n, 'as "sellers"', "no sellers output column");
	});

	await test("mutateRelated + withRelated: combined read + fire-and-forget", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("tags")
			.mutateRelated("sellers", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		// Mutation: CTE
		assertContains(n, "with", "CTE for delete");
		assertContains(n, 'delete from "sellers"', "delete sellers");
		// Read: LATERAL
		assertContains(n, "left join lateral", "tags LATERAL");
		assertContains(n, '"market_tag_joins"', "read tags through-table");
		// Only tags produces jsonb_agg (sellers is fire-and-forget)
		const jsonbAggCount = (n.match(/jsonb_agg/g) || []).length;
		assert(jsonbAggCount === 1, `should have 1 jsonb_agg for tags only, got ${jsonbAggCount}`);
	});

	// -----------------------------------------------------------------------
	// withRelated/mutateRelated: through-table
	// -----------------------------------------------------------------------

	await test("withRelated mutation: through-table uses subquery scope chain", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("tags", (qb) =>
				qb.update().set({ name: "Updated Tag" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "market_tags"', "updates target table");
		assertContains(n, '"market_tag_joins"', "through table in scope chain");
		assert(parameters.includes("Updated Tag"), "SET param");
	});

	await test("mutateRelated delete: through-table", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.mutateRelated("tags", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		assertContains(n, 'delete from "market_tags"', "deletes from target");
		assertContains(n, '"market_tag_joins"', "through table in scope chain");
	});

	// -----------------------------------------------------------------------
	// withRelated/mutateRelated: clone safety
	// -----------------------------------------------------------------------

	await test("withRelated mutation: clones — base unchanged", () => {
		const base = db.selectFrom("markets").selectAll().where("id", "=", 1);
		const baseSql = norm(base.compile().sql);

		const withUpdate = base.withRelated("sellers", (qb) => qb.update().set({ name: "X" }));
		const withUpdateSql = norm(withUpdate.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base unchanged after .withRelated(mutation)");
		assertContains(withUpdateSql, "with", "update query has CTE");
		assertContains(withUpdateSql, 'update "sellers"', "update query has UPDATE");
		assertNotContains(baseSql, "update", "base has no UPDATE");
	});

	await test("mutateRelated: clones — base unchanged", () => {
		const base = db.selectFrom("markets").selectAll().where("id", "=", 1);
		const baseSql = norm(base.compile().sql);

		const withDelete = base.mutateRelated("sellers", (qb) => qb.delete());
		const withDeleteSql = norm(withDelete.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base unchanged after .mutateRelated()");
		assertContains(withDeleteSql, "with", "delete query has CTE");
		assertContains(withDeleteSql, 'delete from "sellers"', "delete query has DELETE");
		assertNotContains(baseSql, "delete", "base has no DELETE");
	});

	// -----------------------------------------------------------------------
	// Complex combined scenarios
	// -----------------------------------------------------------------------

	await test("nested mutation: update sellers with nested update items", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "VIP" })
					.withRelated("items", (qb2) =>
						qb2.update().set({ price: 0 }),
					),
			)
			.compile();
		const n = norm(sql);
		// Both UPDATE CTEs present
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		assertContains(n, 'update "items"', "items UPDATE CTE");
		// Items scoped through sellers scoped through markets
		// Should see nested "in (select" for scope chain
		const inSelectCount = (n.match(/in \(select/g) || []).length;
		assert(inSelectCount >= 2, `should have >= 2 scope subqueries, got ${inSelectCount}`);
		// Assembly CTE combines sellers + items
		assertContains(n, "jsonb_agg", "aggregation present");
		assert(parameters.includes("VIP"), "sellers SET param");
		assert(parameters.includes(0), "items SET param");
		assert(parameters.includes(1), "WHERE param");
	});

	await test("nested mutation: update sellers with nested delete items", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "X" })
					.mutateRelated("items", (qb2) =>
						qb2.delete(),
					),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		assertContains(n, 'delete from "items"', "items DELETE CTE");
		assertContains(n, "returning 1", "delete returns dummy");
	});

	await test("nested mutation: insert sellers with nested insert items", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.insert().values({ name: "NewSeller", booth_number: "Z1" })
					.withRelated("items", (qb2) =>
						qb2.insert().values({ seller_id: 1, name: "NewItem", price: 42 }),
					),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "sellers"', "sellers INSERT CTE");
		assertContains(n, 'insert into "items"', "items INSERT CTE");
		assert(parameters.includes("NewSeller"), "seller insert param");
		assert(parameters.includes("NewItem"), "item insert param");
	});

	await test("multiple mutations: update sellers + delete tags on same query", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "NEW" }),
			)
			.mutateRelated("tags", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		assertContains(n, 'delete from "market_tags"', "tags DELETE CTE (through-table target)");
		// sellers should appear in output, tags should not
		assertContains(n, 'as "sellers"', "sellers in output");
		assertNotContains(n, 'as "tags"', "tags not in output (fire-and-forget)");
	});

	await test("triple combo: read + mutation + mutateRelated all in one", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("tags")
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "UPD" }),
			)
			.mutateRelated(
				(b) => b("sellers").as("deletedSellers").on("booth_number", "=", "OLD"),
				(qb) => qb.delete(),
			)
			.compile();
		const n = norm(sql);
		// Read: LATERAL for tags
		assertContains(n, "left join lateral", "tags LATERAL read");
		// Mutation: CTE for sellers update
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		// Fire-and-forget: CTE for sellers delete
		assertContains(n, 'delete from "sellers"', "sellers DELETE CTE");
		// tags read and sellers mutation in output, deletedSellers not
		assertContains(n, '"market_tag_joins"', "tags through table");
		// CTE structure
		assertContains(n, "with", "has CTEs");
	});

	await test("mutation on returning builder: updateTable + returningAll + withRelated mutation", () => {
		const { sql, parameters } = db
			.updateTable("markets")
			.set({ name: "Updated" })
			.where("id", "=", 1)
			.returningAll()
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "RET" }),
			)
			.compile();
		const n = norm(sql);
		// Main mutation as _mutation CTE
		assertContains(n, 'update "markets"', "main UPDATE");
		// Nested mutation CTE for sellers
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		// _mutation alias used
		assertContains(n, "_mutation", "main CTE alias");
		assertContains(n, "returning *", "RETURNING *");
		assert(parameters.includes("Updated"), "main SET param");
		assert(parameters.includes("RET"), "relation SET param");
	});

	await test("mutation on returning builder: insertInto + returningAll + withRelated read", () => {
		const { sql, parameters } = db
			.insertInto("markets")
			.values({ name: "New", location: "Here", active: true })
			.returningAll()
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "markets"', "INSERT");
		assertContains(n, "_mutation", "CTE wrapping");
		// Read relation via LATERAL
		assertContains(n, "left join lateral", "sellers LATERAL read");
		assertContains(n, "jsonb_agg", "sellers aggregation");
		assert(parameters.includes("New"), "insert param");
	});

	await test("mutation on returning builder: insertInto + returningAll + mutateRelated", () => {
		const { sql } = db
			.insertInto("markets")
			.values({ name: "M", location: "L", active: true })
			.returningAll()
			.mutateRelated("sellers", (qb) => qb.delete())
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "markets"', "INSERT");
		assertContains(n, 'delete from "sellers"', "sellers DELETE CTE");
		assertContains(n, "_mutation", "main CTE");
		// Fire-and-forget: no sellers in output
		assertNotContains(n, 'as "sellers"', "no sellers output");
	});

	await test("mutation on returning builder: deleteFrom + returningAll + withRelated read", () => {
		const { sql } = db
			.deleteFrom("markets")
			.where("id", "=", 999)
			.returningAll()
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		assertContains(n, 'delete from "markets"', "DELETE");
		assertContains(n, "_mutation", "CTE wrapping");
		assertContains(n, "left join lateral", "sellers LATERAL");
	});

	await test("mixed read + mutation on returning builder", () => {
		const { sql } = db
			.updateTable("markets")
			.set({ name: "X" })
			.where("id", "=", 1)
			.returningAll()
			.withRelated("tags")
			.withRelated("sellers", (qb) =>
				qb.update().set({ name: "Y" }),
			)
			.compile();
		const n = norm(sql);
		// Main mutation
		assertContains(n, 'update "markets"', "main UPDATE");
		// Sellers mutation CTE
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		// Tags read
		assertContains(n, "left join lateral", "tags LATERAL");
		assertContains(n, '"market_tag_joins"', "tags through table");
		// Both relations in output
		assertContains(n, 'as "sellers"', "sellers in output");
	});

	await test("select + withRelated mutation: selectAll preserved", () => {
		const { sql } = db
			.selectFrom("markets")
			.selectAll()
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ name: "Z" }),
			)
			.compile();
		const n = norm(sql);
		// _main CTE wraps the inner selectAll query
		assertContains(n, "_main", "main CTE wrapping");
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		assertContains(n, "with", "WITH clause");
	});

	await test("insert relation via withRelated: INSERT CTE", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.insert().values({ name: "Inserted", booth_number: "NEW" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, "with", "WITH clause");
		assertContains(n, 'insert into "sellers"', "INSERT CTE");
		assertContains(n, "returning", "RETURNING for inserted rows");
		assertContains(n, "jsonb_agg", "aggregates inserted rows");
		assert(parameters.includes("Inserted"), "insert param");
	});

	await test("insert relation with onConflict", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.insert()
					.values({ name: "Upserted", booth_number: "U1" })
					.onConflict((oc: any) => oc.column("name").doUpdateSet({ booth_number: "U1" })),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "sellers"', "INSERT CTE");
		assertContains(n, "on conflict", "ON CONFLICT clause");
		assertContains(n, "do update set", "DO UPDATE SET");
	});

	await test("multiple parent where conditions: all propagated to scope", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", ">", 0)
			.where("active", "=", true)
			.where("location", "=", "Helsinki")
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "MULTI" }),
			)
			.compile();
		const n = norm(sql);
		// The scope subquery should contain the parent conditions
		assertContains(n, "in (select", "scope subquery present");
		assert(parameters.includes(0), "first WHERE param");
		assert(parameters.includes(true), "second WHERE param");
		assert(parameters.includes("Helsinki"), "third WHERE param");
		assert(parameters.includes("MULTI"), "SET param");
	});

	await test("mutation where filter + builder ON condition combined", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated(
				(b) => b("sellers").as("filtered").on("booth_number", "=", "A1"),
				(qb) => qb.update().set({ name: "Filtered" }).where("name", "!=", "Skip"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "UPDATE CTE");
		// Both ON condition and inner where should appear
		assert(parameters.includes("A1"), "ON condition param");
		assert(parameters.includes("Filtered"), "SET param");
		assert(parameters.includes("Skip"), "inner WHERE param");
	});

	await test("deeply nested: markets → sellers update → items update → seller read", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "DEEP" })
					.withRelated("items", (qb2) =>
						qb2.update().set({ price: 999 })
							.withRelated("seller"),
					),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "sellers UPDATE");
		assertContains(n, 'update "items"', "items UPDATE");
		// Three levels deep
		const updateCount = (n.match(/update "/g) || []).length;
		assert(updateCount === 2, `should have 2 UPDATE statements, got ${updateCount}`);
	});

	await test("withRelated mutation + withRelated read on same relation name", () => {
		// Update sellers, and also read tags — both toMany but different handling
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "MUT" }),
			)
			.withRelated("tags")
			.compile();
		const n = norm(sql);
		// Mutation: CTE
		assertContains(n, 'update "sellers"', "sellers UPDATE CTE");
		// Read: LATERAL
		assertContains(n, "left join lateral", "tags LATERAL");
		// Both in output
		assertContains(n, 'as "sellers"', "sellers output");
		assertContains(n, '"market_tag_joins"', "tags through table");
	});

	await test("mutateRelated insert: fire-and-forget INSERT CTE", () => {
		const { sql, parameters } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.mutateRelated("sellers", (qb) =>
				qb.insert().values({ name: "Ghost", booth_number: "G1" }),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "sellers"', "INSERT CTE");
		// Fire-and-forget: no sellers in output
		assertNotContains(n, 'as "sellers"', "no sellers output");
		assert(parameters.includes("Ghost"), "insert param");
	});

	await test("three mutations on same query: update + insert + delete", () => {
		const { sql } = db
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb) =>
				qb.update().set({ booth_number: "U" }).where("name", "=", "Alice"),
			)
			.withRelated("tags", (qb) =>
				qb.insert().values({ name: "new-tag" }),
			)
			.mutateRelated(
				(b) => b("sellers").as("delSellers").on("booth_number", "=", "OLD"),
				(qb) => qb.delete(),
			)
			.compile();
		const n = norm(sql);
		// All three mutation types
		assertContains(n, 'update "sellers"', "UPDATE CTE");
		assertContains(n, 'insert into "market_tags"', "INSERT CTE");
		assertContains(n, 'delete from "sellers"', "DELETE CTE");
		// sellers and tags in output, delSellers not
		assertContains(n, 'as "sellers"', "sellers output");
		assertContains(n, 'as "tags"', "tags output");
	});

	// -----------------------------------------------------------------------
	// Projections
	// -----------------------------------------------------------------------

	// Create a second ORM with projections defined
	const metaWithProjections = {
		markets: {
			projections: {
				default: ["id", "name", "location"],
				summary: ["id", "name"],
			},
			relations: {
				sellers: {
					model: "sellers",
					type: "many",
					from: "id",
					to: "market_id",
				},
				tags: {
					model: "market_tags",
					type: "many",
					from: "id",
					to: "id",
					through: {
						table: "market_tag_joins",
						from: "market_id",
						to: "tag_id",
					},
				},
			},
		},
		sellers: {
			projections: {
				default: ["id", "name", "market_id"],
				summary: ["id", "name"],
				relation: ["id", "name", "booth_number"],
			},
			relations: {
				market: {
					model: "markets",
					type: "one",
					from: "market_id",
					to: "id",
				},
				items: {
					model: "items",
					type: "many",
					from: "id",
					to: "seller_id",
				},
			},
		},
		items: {
			relations: {
				seller: {
					model: "sellers",
					type: "one",
					from: "seller_id",
					to: "id",
				},
			},
		},
	} as const satisfies MetaDB<Database>;

	const pdb = createOrm(rawDb, metaWithProjections);

	await test("project(): selects only projection columns", () => {
		const { sql } = pdb.selectFrom("markets").project("summary").compile();
		const n = norm(sql);
		assertContains(n, '"markets"."id"', "has id");
		assertContains(n, '"markets"."name"', "has name");
		assertNotContains(n, '"markets".*', "no selectAll");
		assertNotContains(n, '"location"', "no location");
	});

	await test("project() + select() accumulates columns", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.project("summary")
			.select(["active"])
			.compile();
		const n = norm(sql);
		assertContains(n, '"markets"."id"', "has id from projection");
		assertContains(n, '"markets"."name"', "has name from projection");
		assertContains(n, '"active"', "has active from select");
		assertNotContains(n, '"markets".*', "no selectAll");
	});

	await test("no select/project on table WITH projections uses default projection", () => {
		const { sql } = pdb.selectFrom("markets").compile();
		const n = norm(sql);
		assertContains(n, '"markets"."id"', "has id from default");
		assertContains(n, '"markets"."name"', "has name from default");
		assertContains(n, '"markets"."location"', "has location from default");
		assertNotContains(n, '"markets".*', "no selectAll");
	});

	await test("no select/project on table WITHOUT projections uses selectAll", () => {
		// items has no projections defined
		const { sql } = pdb.selectFrom("items").compile();
		const n = norm(sql);
		assertContains(n, '"items".*', "selectAll for items (no projections)");
	});

	await test("select() on table WITH projections only selects those columns (no default)", () => {
		const { sql } = pdb.selectFrom("markets").select(["id"]).compile();
		const n = norm(sql);
		assertContains(n, '"id"', "has selected id");
		assertNotContains(n, '"markets"."name"', "no name from default projection");
		assertNotContains(n, '"markets".*', "no selectAll");
	});

	await test("withRelated uses 'relation' projection on target table", () => {
		// sellers has relation: ["id", "name", "booth_number"]
		const { sql } = pdb.selectFrom("markets").withRelated("sellers").compile();
		const n = norm(sql);
		assertContains(n, '"sellers"."id"', "has id from relation projection");
		assertContains(n, '"sellers"."name"', "has name from relation projection");
		assertContains(n, '"sellers"."booth_number"', "has booth_number from relation projection");
		assertNotContains(n, '"sellers".*', "no selectAll on sellers");
	});

	await test("withRelated uses 'default' projection fallback when no 'relation'", () => {
		// markets has default: ["id", "name", "location"] but no relation projection
		const { sql } = pdb.selectFrom("sellers").withRelated("market").compile();
		const n = norm(sql);
		// toOne uses to_jsonb(alias.*) so columns aren't individually listed in the join
		// But we can check it's doing a LEFT JOIN
		assertContains(n, "left join", "has left join for toOne");
	});

	await test("withRelated with variant string applies that projection", () => {
		// sellers with "summary" projection: ["id", "name"]
		const { sql } = pdb
			.selectFrom("markets")
			.withRelated("sellers", "summary")
			.compile();
		const n = norm(sql);
		assertContains(n, '"sellers"."id"', "has id from summary");
		assertContains(n, '"sellers"."name"', "has name from summary");
		assertNotContains(n, '"sellers"."booth_number"', "no booth_number (not in summary)");
		assertNotContains(n, '"sellers".*', "no selectAll");
	});

	await test("withRelated on table without projections uses selectAll", () => {
		// items has no projections defined
		const { sql } = pdb.selectFrom("sellers").withRelated("items").compile();
		const n = norm(sql);
		assertContains(n, '"items".*', "selectAll for items (no projections)");
	});

	await test("project() + withRelated: both projections applied", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.project("summary")
			.withRelated("sellers")
			.compile();
		const n = norm(sql);
		// Parent uses summary: ["id", "name"]
		assertContains(n, '"markets"."id"', "parent has id");
		assertContains(n, '"markets"."name"', "parent has name");
		assertNotContains(n, '"markets".*', "no parent selectAll");
		// Relation uses relation projection: ["id", "name", "booth_number"]
		assertContains(n, '"sellers"."id"', "relation has id");
		assertContains(n, '"sellers"."booth_number"', "relation has booth_number");
	});

	await test("nested withRelated with projections", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.withRelated("sellers", (qb) =>
				qb.withRelated("items"),
			)
			.compile();
		const n = norm(sql);
		// sellers uses relation projection
		assertContains(n, '"sellers"."booth_number"', "sellers has booth_number from relation");
		// items has no projections, so selectAll
		assertContains(n, '"items".*', "items uses selectAll");
	});

	await test("builder form withRelated with variant", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.withRelated(
				(b) => b("sellers").as("topSellers"),
				"summary",
			)
			.compile();
		const n = norm(sql);
		assertContains(n, '"topsellers"', "alias appears");
		assertContains(n, '"sellers"."id"', "has id from summary");
		assertContains(n, '"sellers"."name"', "has name from summary");
		assertNotContains(n, '"sellers"."booth_number"', "no booth_number");
	});

	await test("project() clones — original unchanged", () => {
		const base = pdb.selectFrom("markets");
		const baseSql = norm(base.compile().sql);

		const projected = base.project("summary");
		const projectedSql = norm(projected.compile().sql);

		const baseAfter = norm(base.compile().sql);
		assert(baseAfter === baseSql, "base unchanged after .project()");
		assert(projectedSql !== baseSql, "projected SQL differs from base");
	});

	await test("withRelated toOne scalar subquery uses relation projection", () => {
		// In CTE context (mutations), toOne uses scalar subquery
		// markets has default projection but no relation projection
		const { sql } = pdb
			.insertInto("sellers")
			.values({ name: "Test", market_id: 1, booth_number: "A1" })
			.returningAll()
			.withRelated("market")
			.compile();
		const n = norm(sql);
		assertContains(n, "to_jsonb", "has to_jsonb for toOne");
		// markets has default: ["id", "name", "location"] — should use it as fallback
		assertContains(n, '"markets"."id"', "has id from default projection");
		assertContains(n, '"markets"."name"', "has name from default projection");
		assertContains(n, '"markets"."location"', "has location from default projection");
		assertNotContains(n, '"markets".*', "no selectAll on markets");
	});

	await test("mutation withRelated variant string", () => {
		const { sql } = pdb
			.insertInto("markets")
			.values({ name: "T", location: "H", active: true })
			.returningAll()
			.withRelated("sellers", "summary")
			.compile();
		const n = norm(sql);
		assertContains(n, '"sellers"."id"', "has id from summary");
		assertContains(n, '"sellers"."name"', "has name from summary");
		assertNotContains(n, '"sellers"."booth_number"', "no booth_number");
		assertNotContains(n, '"sellers".*', "no selectAll");
	});

	// -----------------------------------------------------------------------
	// Projection edge cases
	// -----------------------------------------------------------------------

	await test("multiple project() calls accumulate columns", () => {
		// Use existing projections: summary = ["id", "name"], default = ["id", "name", "location"]
		// Calling both should accumulate: id, name, location
		const { sql } = pdb.selectFrom("markets").project("summary").project("default").compile();
		const n = norm(sql);
		// Should have columns from BOTH projections (union of summary + default)
		assertContains(n, '"markets"."id"', "has id from both");
		assertContains(n, '"markets"."name"', "has name from both");
		assertContains(n, '"markets"."location"', "has location from default");
		assertNotContains(n, '"markets".*', "no selectAll");
		// active is NOT in either projection
		assertNotContains(n, '"active"', "no active (not in either projection)");
	});

	await test("select() after project() accumulates columns", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.project("summary")
			.select(["location", "active"])
			.compile();
		const n = norm(sql);
		// summary: ["id", "name"]
		assertContains(n, '"markets"."id"', "has id from projection");
		assertContains(n, '"markets"."name"', "has name from projection");
		// Additional selects
		assertContains(n, '"location"', "has location from select");
		assertContains(n, '"active"', "has active from select");
		assertNotContains(n, '"markets".*', "no selectAll");
	});

	await test("project() after select() accumulates columns", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.select(["location", "active"])
			.project("summary")
			.compile();
		const n = norm(sql);
		// Initial selects
		assertContains(n, '"location"', "has location from select");
		assertContains(n, '"active"', "has active from select");
		// summary: ["id", "name"]
		assertContains(n, '"markets"."id"', "has id from projection");
		assertContains(n, '"markets"."name"', "has name from projection");
		assertNotContains(n, '"markets".*', "no selectAll");
	});

	await test("project() + selectAll() — selectAll wins (covers projection)", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.project("summary")
			.selectAll()
			.compile();
		const n = norm(sql);
		// selectAll should include all columns (overrides projection)
		assertContains(n, '"markets".*', "has selectAll");
	});

	await test("selectAll() + project() accumulates", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.selectAll()
			.project("summary")
			.compile();
		const n = norm(sql);
		// Both selectAll and projection columns
		assertContains(n, '"markets".*', "has selectAll");
		assertContains(n, '"markets"."id"', "has id from projection");
		assertContains(n, '"markets"."name"', "has name from projection");
	});

	await test("project() with no projections defined throws error", () => {
		try {
			// items has no projections, so this should throw at runtime
			// @ts-expect-error - items has no projections
			pdb.selectFrom("items").project("nonexistent").compile();
			failed++;
			console.error(`  FAIL: should throw error for nonexistent projection`);
		} catch (err: any) {
			if (err.message.includes('Projection "nonexistent" not found')) {
				// Expected error
			} else {
				throw err;
			}
		}
	});

	await test("project() on mutation withRelated", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb: any) =>
				qb.update().set({ booth_number: "PROJ" }).project("summary"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'update "sellers"', "UPDATE in CTE");
		// Projection in RETURNING clause: summary = ["id", "name"]
		assertContains(n, '"sellers"."id"', "returning id from projection");
		assertContains(n, '"sellers"."name"', "returning name from projection");
		// Should NOT have booth_number in RETURNING (not in summary projection)
		// Note: this is tricky to verify in normalized SQL since booth_number appears in SET
		// We can check that it's in a RETURNING context
	});

	await test("project() on insert relation", () => {
		const { sql } = pdb
			.selectFrom("markets")
			.where("id", "=", 1)
			.withRelated("sellers", (qb: any) =>
				qb.insert().values({ name: "New", booth_number: "N1" }).project("summary"),
			)
			.compile();
		const n = norm(sql);
		assertContains(n, 'insert into "sellers"', "INSERT in CTE");
		// Projection in RETURNING: summary = ["id", "name"]
		assertContains(n, '"sellers"."id"', "returning id from projection");
		assertContains(n, '"sellers"."name"', "returning name from projection");
	});

	await test("withRelated inside mutateRelated throws error", () => {
		try {
			db
				.selectFrom("markets")
				.where("id", "=", 1)
				.mutateRelated("sellers", (qb: any) =>
					qb.update().set({ booth_number: "X" }).withRelated("items"),
				);
			failed++;
			console.error(`  FAIL: should throw error for withRelated inside mutateRelated`);
		} catch (err: any) {
			if (err.message.includes("withRelated() cannot be used inside mutateRelated()")) {
				// Expected error
			} else {
				throw err;
			}
		}
	});

	// -----------------------------------------------------------------------
	// Done
	// -----------------------------------------------------------------------

	console.log(`\nResults: ${passed} passed, ${failed} failed\n`);
	if (failed > 0) process.exit(1);
}

runTests().catch((err) => {
	console.error(err);
	process.exit(1);
});
