/**
 * Type-level tests for kysely-is-an-orm.
 * Run with: tsc --noEmit
 *
 * Lines marked @ts-expect-error MUST produce a compiler error.
 * If they don't, tsc will report "Unused '@ts-expect-error' directive"
 * which itself is an error — so the test fails.
 *
 * Lines without @ts-expect-error MUST compile cleanly.
 */

import type {
	Equals,
	Expect,
	Extends,
	HasKey,
	NotHasKey,
} from "type-test-core";
import { Kysely, type ColumnType, type Generated, type Selectable } from "kysely";
import { createOrm, type MetaDB } from "../src/index.js";

// ============================================================================
// Test database schema
// ============================================================================

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

type Timestamp = ColumnType<Date, Date | string, Date | string>;

interface UsagePeriodTable {
	tenant_id: string;
	period_start: Timestamp;
	metric: string;
	note: string | null;
	count: Generated<number>;
	updated_at: Generated<Timestamp>;
}

interface Database {
	markets: MarketTable;
	sellers: SellerTable;
	items: ItemTable;
	market_tags: MarketTagTable;
	market_tag_joins: MarketTagJoinTable;
	usage_periods: UsagePeriodTable;
}

// ============================================================================
// 1. Meta definition: typed columns and tables
// ============================================================================

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

// Meta should catch wrong column names
const _badMeta1 = {
	markets: {
		relations: {
			sellers: {
				model: "sellers",
				type: "many",
				// @ts-expect-error - "nonexistent" is not a column of markets
				from: "nonexistent",
				to: "market_id",
			},
		},
	},
} satisfies MetaDB<Database>;

const _badMeta2 = {
	markets: {
		relations: {
			sellers: {
				model: "sellers",
				type: "many",
				from: "id",
				// @ts-expect-error - "nonexistent" is not a column of sellers
				to: "nonexistent",
			},
		},
	},
} satisfies MetaDB<Database>;

// Meta should catch wrong table name in model
const _badMeta3 = {
	markets: {
		relations: {
			bad: {
				// @ts-expect-error - "nonexistent_table" is not a table
				model: "nonexistent_table",
				type: "many",
				from: "id",
				to: "id",
			},
		},
	},
} satisfies MetaDB<Database>;

// Meta should type through.from and through.to as columns of the through table
const _badMeta4 = {
	markets: {
		relations: {
			tags: {
				model: "market_tags",
				type: "many",
				from: "id",
				to: "id",
				through: {
					table: "market_tag_joins",
					// @ts-expect-error - "nonexistent" is not a column of market_tag_joins
					from: "nonexistent",
					to: "tag_id",
				},
			},
		},
	},
} satisfies MetaDB<Database>;

const _badMeta5 = {
	markets: {
		relations: {
			tags: {
				model: "market_tags",
				type: "many",
				from: "id",
				to: "id",
				through: {
					table: "market_tag_joins",
					from: "market_id",
					// @ts-expect-error - "nonexistent" is not a column of market_tag_joins
					to: "nonexistent",
				},
			},
		},
	},
} satisfies MetaDB<Database>;

// ============================================================================
// 2. createOrm and selectFrom
// ============================================================================

// Use null! as a type-only Kysely instance (no runtime needed for type tests)
const db = createOrm(null! as Kysely<Database>, meta);

// selectFrom accepts valid table names
const _qMarkets = db.selectFrom("markets");
const _qSellers = db.selectFrom("sellers");

// @ts-expect-error - invalid table name
const _qInvalid = db.selectFrom("nonexistent");

// ============================================================================
// 3. withRelated result types: many = array, one = T | null
// ============================================================================

async function testWithRelatedResultTypes() {
	// many relation: sellers should be an array
	const marketsWithSellers = await db
		.selectFrom("markets")
		.withRelated("sellers")
		.execute();

	type MWS = typeof marketsWithSellers[0];
	type _T2 = Expect<HasKey<MWS, "sellers">>;
	type _T3 = Expect<Extends<MWS["sellers"], Selectable<SellerTable>[]>>;

	// one relation: market should be T | null
	const sellersWithMarket = await db
		.selectFrom("sellers")
		.withRelated("market")
		.execute();

	type SWM = typeof sellersWithMarket[0];
	type _T5 = Expect<HasKey<SWM, "market">>;
	type _T6 = Expect<Extends<SWM["market"], Selectable<MarketTable> | null>>;

	// Without explicit select(), base columns ARE included (default to all)
	type _T7 = Expect<HasKey<MWS, "id">>;
	type _T8 = Expect<HasKey<MWS, "name">>;

	// With explicit selectAll(), same behavior — base columns included
	const marketsAll = await db
		.selectFrom("markets")
		.selectAll()
		.withRelated("sellers")
		.execute();

	type MA = typeof marketsAll[0];
	type _T9 = Expect<HasKey<MA, "id">>;
	type _T10 = Expect<HasKey<MA, "name">>;
	type _T11 = Expect<HasKey<MA, "sellers">>;
}

// ============================================================================
// 4. select() narrows output type
// ============================================================================

async function testSelectNarrowing() {
	// select() with array narrows to just those columns
	const narrow = await db
		.selectFrom("markets")
		.select(["id", "name"])
		.execute();

	type NarrowRow = typeof narrow[0];
	type _S1 = Expect<HasKey<NarrowRow, "id">>;
	type _S2 = Expect<HasKey<NarrowRow, "name">>;
	// location and created_at should NOT be in the narrowed result
	type _S3 = Expect<NotHasKey<NarrowRow, "location">>;
	type _S4 = Expect<NotHasKey<NarrowRow, "created_at">>;

	// select() with single column
	const single = await db
		.selectFrom("markets")
		.select("id")
		.execute();

	type SingleRow = typeof single[0];
	type _S5 = Expect<HasKey<SingleRow, "id">>;
	type _S6 = Expect<NotHasKey<SingleRow, "name">>;

	// select() + withRelated: narrowed columns + relation
	const narrowWithRel = await db
		.selectFrom("markets")
		.select(["id", "name"])
		.withRelated("sellers")
		.execute();

	type NarrowWithRelRow = typeof narrowWithRel[0];
	type _S7 = Expect<HasKey<NarrowWithRelRow, "id">>;
	type _S8 = Expect<HasKey<NarrowWithRelRow, "name">>;
	type _S9 = Expect<HasKey<NarrowWithRelRow, "sellers">>;
	type _S10 = Expect<NotHasKey<NarrowWithRelRow, "location">>;
}

// ============================================================================
// 5. Invalid relation names are rejected
// ============================================================================

// @ts-expect-error - "nonexistent" is not a relation on markets
const _invalidRel1 = db.selectFrom("markets").withRelated("nonexistent");

// @ts-expect-error - "market" is a relation on sellers, not markets
const _invalidRel2 = db.selectFrom("markets").withRelated("market");

// @ts-expect-error - "sellers" is a relation on markets, not items
const _invalidRel3 = db.selectFrom("items").withRelated("sellers");

// Builder API: invalid relation names
const _invalidRelCb = db.selectFrom("markets")
	// @ts-expect-error - "nonexistent" is not a relation on markets
	.withRelated((b) => b("nonexistent").as("bad"));

const _invalidRelCb2 = db.selectFrom("markets")
	// @ts-expect-error - "market" is not a relation on markets
	.withRelated((b) => b("market").as("bad"));

// ============================================================================
// 6. Column names in where() autocomplete correctly (no any leakage)
// ============================================================================

// Valid columns
const _w1 = db.selectFrom("markets").where("id", "=", 1);
const _w2 = db.selectFrom("markets").where("name", "=", "test");
const _w3 = db.selectFrom("sellers").where("market_id", "=", 1);
const _w4 = db.selectFrom("sellers").where("booth_number", "=", "A1");

// @ts-expect-error - invalid column name
const _wInvalid1 = db.selectFrom("markets").where("nonexistent", "=", 1);

// @ts-expect-error - column from wrong table
const _wInvalid2 = db.selectFrom("markets").where("booth_number", "=", "A1");

// orderBy should also type-check columns
const _ob1 = db.selectFrom("markets").orderBy("name", "asc");

// ============================================================================
// 7. Nested withRelated types work
// ============================================================================

async function testNestedWithRelated() {
	// markets -> sellers -> items (two levels deep)
	const result = await db
		.selectFrom("markets")
		.withRelated("sellers", (qb) =>
			qb.withRelated("items"),
		)
		.execute();

	type Row = typeof result[0];
	type _N2 = Expect<HasKey<Row, "sellers">>;

	// sellers -> market (back-reference)
	const result2 = await db
		.selectFrom("sellers")
		.withRelated("market")
		.execute();
	type Row2 = typeof result2[0];
	type _N3 = Expect<HasKey<Row2, "market">>;

	// Modifier qb should have correct columns for the target table
	db.selectFrom("markets")
		.withRelated("sellers", (qb) =>
			qb.where("booth_number", "=", "A1").withRelated("items"),
		);

	db.selectFrom("markets")
		.withRelated("sellers", (qb) =>
			// @ts-expect-error - "location" is not a column on sellers
			qb.where("location", "=", "Helsinki"),
		);
}

// ============================================================================
// 7b. Native Kysely joins remain available on the ORM select builder
// ============================================================================

async function testNativeJoinTypes() {
	const rows = await db
		.selectFrom("sellers")
		.innerJoin("markets", "markets.id", "sellers.market_id")
		.leftJoin("items", "items.seller_id", "sellers.id")
		.select([
			"sellers.id as seller_id",
			"markets.name as market_name",
			"items.name as item_name",
		])
		.where("markets.active", "=", true)
		.execute();

	type Row = typeof rows[0];
	type _J1 = Expect<HasKey<Row, "seller_id">>;
	type _J2 = Expect<HasKey<Row, "market_name">>;
	type _J3 = Expect<HasKey<Row, "item_name">>;

	db.selectFrom("sellers")
		.innerJoin("markets", "markets.id", "sellers.market_id")
		// @ts-expect-error - joined table has no such column
		.select("markets.not_a_column");
}

// ============================================================================
// 8. Builder callback: alias, ON conditions
// ============================================================================

async function testBuilderCallback() {
	// Aliased relation via builder
	const result = await db
		.selectFrom("markets")
		.withRelated(
			(b) => b("sellers").as("boothA"),
			(qb) => qb.where("booth_number", "like", "A%"),
		)
		.withRelated(
			(b) => b("sellers").as("boothB"),
			(qb) => qb.where("booth_number", "like", "B%"),
		)
		.execute();

	type Row = typeof result[0];
	type _A2 = Expect<HasKey<Row, "boothA">>;
	type _A3 = Expect<HasKey<Row, "boothB">>;
	// The original "sellers" key should NOT exist (only aliases)
	type _A4 = Expect<NotHasKey<Row, "sellers">>;

	// Aliased result should have correct type (sellers is many, so array)
	type _A5 = Expect<Extends<Row["boothA"], Selectable<SellerTable>[]>>;
	type _A6 = Expect<Extends<Row["boothB"], Selectable<SellerTable>[]>>;

	// Aliased one relation
	const result2 = await db
		.selectFrom("sellers")
		.withRelated(
			(b) => b("market").as("parentMarket"),
		)
		.execute();

	type Row2 = typeof result2[0];
	type _A7 = Expect<HasKey<Row2, "parentMarket">>;
	type _A8 = Expect<NotHasKey<Row2, "market">>;
	type _A9 = Expect<Extends<Row2["parentMarket"], Selectable<MarketTable> | null>>;

	// Builder with .on() — extra ON conditions
	db.selectFrom("sellers")
		.withRelated(
			(b) => b("market").on("active", "=", true),
		);

	// Builder with .inner() — INNER JOIN instead of LEFT
	db.selectFrom("sellers")
		.withRelated(
			(b) => b("market").inner(),
		);

	// Builder with .onRef() — reference source table column
	db.selectFrom("sellers")
		.withRelated(
			(b) => b("market").onRef("id", "=", "market_id"),
		);
}

// ============================================================================
// 9. compile() types
// ============================================================================

function testCompile() {
	const compiled = db.selectFrom("markets").withRelated("sellers").compile();

	type _C2 = Expect<HasKey<typeof compiled, "sql">>;
	type _C3 = Expect<HasKey<typeof compiled, "parameters">>;
	type _C4 = Expect<Equals<string, typeof compiled.sql>>;
	type _C5 = Expect<Extends<typeof compiled.parameters, readonly unknown[]>>;
}

// ============================================================================
// 10. Chaining preserves types
// ============================================================================

async function testChaining() {
	// selectAll + where + orderBy + limit + withRelated
	const result = await db
		.selectFrom("markets")
		.selectAll()
		.where("location", "=", "Helsinki")
		.orderBy("name", "asc")
		.limit(10)
		.withRelated("sellers")
		.execute();

	type Row = typeof result[0];
	type _CH2 = Expect<HasKey<Row, "id">>;
	type _CH3 = Expect<HasKey<Row, "sellers">>;

	// withRelated + where (order shouldn't matter)
	const result2 = await db
		.selectFrom("markets")
		.withRelated("sellers")
		.where("id", "=", 1)
		.execute();

	type Row2 = typeof result2[0];
	type _CH4 = Expect<HasKey<Row2, "sellers">>;
}

// ============================================================================
// 11. executeTakeFirst returns T | undefined, executeTakeFirstOrThrow returns T
// ============================================================================

async function testExecuteTakeFirst() {
	const maybe = await db
		.selectFrom("markets")
		.selectAll()
		.executeTakeFirst();

	type MaybeRow = typeof maybe;
	type _E1 = Expect<Extends<undefined, MaybeRow>>;

	const definite = await db
		.selectFrom("markets")
		.selectAll()
		.executeTakeFirstOrThrow();

	type DefiniteRow = typeof definite;
	// Should NOT include undefined
	type _E2 = Expect<HasKey<DefiniteRow, "id">>;

	// executeTakeFirstOrThrow with custom error
	const _withError = await db
		.selectFrom("markets")
		.selectAll()
		.executeTakeFirstOrThrow(new Error("not found"));

	const _withFactory = await db
		.selectFrom("markets")
		.selectAll()
		.executeTakeFirstOrThrow(() => new Error("not found"));
}

// ============================================================================
// 12. insertInto type safety
// ============================================================================

async function testInsertTypes() {
	// insertInto accepts valid table names
	const _q = db.insertInto("markets");

	// @ts-expect-error - invalid table
	const _qBad = db.insertInto("nonexistent");

	// values() requires Insertable columns
	db.insertInto("markets").values({ name: "Test", location: "Helsinki", active: true });

	// @ts-expect-error - missing required column "name"
	db.insertInto("markets").values({ location: "Helsinki", active: true });

	// returningAll() produces OrmReturningBuilder
	const returning = db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returningAll();

	// returningAll + execute returns Selectable<MarketTable>
	const inserted = await returning.execute();
	type InsertedRow = typeof inserted[0];
	type _I1 = Expect<HasKey<InsertedRow, "id">>;
	type _I2 = Expect<HasKey<InsertedRow, "name">>;
	type _I3 = Expect<HasKey<InsertedRow, "active">>;

	// returning() narrows root mutation output to explicit columns
	const insertedId = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returning("id")
		.execute();

	type InsertedIdRow = typeof insertedId[0];
	type _I12 = Expect<HasKey<InsertedIdRow, "id">>;
	type _I13 = Expect<NotHasKey<InsertedIdRow, "name">>;
	type _I14 = Expect<NotHasKey<InsertedIdRow, "active">>;

	const insertedSummary = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returning(["id", "name"])
		.execute();

	type InsertedSummaryRow = typeof insertedSummary[0];
	type _I15 = Expect<HasKey<InsertedSummaryRow, "id">>;
	type _I16 = Expect<HasKey<InsertedSummaryRow, "name">>;
	type _I17 = Expect<NotHasKey<InsertedSummaryRow, "location">>;

	const insertedChainedReturning = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returning("id")
		.returning("name")
		.execute();

	type InsertedChainedReturningRow = typeof insertedChainedReturning[0];
	type _I23 = Expect<HasKey<InsertedChainedReturningRow, "id">>;
	type _I24 = Expect<HasKey<InsertedChainedReturningRow, "name">>;
	type _I25 = Expect<NotHasKey<InsertedChainedReturningRow, "active">>;

	// @ts-expect-error - invalid returned column
	db.insertInto("markets").values({ name: "T", location: "H", active: true }).returning("nonexistent");

	// returningAll + withRelated
	const withRel = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returningAll()
		.withRelated("sellers")
		.execute();

	type WithRelRow = typeof withRel[0];
	type _I4 = Expect<HasKey<WithRelRow, "id">>;
	type _I5 = Expect<HasKey<WithRelRow, "name">>;
	type _I6 = Expect<HasKey<WithRelRow, "sellers">>;
	type _I7 = Expect<Extends<WithRelRow["sellers"], Selectable<SellerTable>[]>>;

	// withRelated toOne
	const sellerInsert = await db
		.insertInto("sellers")
		.values({ name: "Test", market_id: 1, booth_number: "A1" })
		.returningAll()
		.withRelated("market")
		.execute();

	type SellerInsRow = typeof sellerInsert[0];
	type _I8 = Expect<HasKey<SellerInsRow, "market">>;
	type _I9 = Expect<Extends<SellerInsRow["market"], Selectable<MarketTable> | null>>;

	// returning() + withRelated keeps root columns narrowed and relation types present
	const sellerInsertIdWithMarket = await db
		.insertInto("sellers")
		.values({ name: "Test", market_id: 1, booth_number: "A1" })
		.returning("id")
		.withRelated("market")
		.execute();

	type SellerInsertIdWithMarketRow = typeof sellerInsertIdWithMarket[0];
	type _I18 = Expect<HasKey<SellerInsertIdWithMarketRow, "id">>;
	type _I19 = Expect<NotHasKey<SellerInsertIdWithMarketRow, "name">>;
	type _I20 = Expect<NotHasKey<SellerInsertIdWithMarketRow, "market_id">>;
	type _I21 = Expect<HasKey<SellerInsertIdWithMarketRow, "market">>;
	type _I22 = Expect<Extends<SellerInsertIdWithMarketRow["market"], Selectable<MarketTable> | null>>;

	const insertedChainedReturningWithRel = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returning("id")
		.returning("name")
		.withRelated("sellers")
		.execute();

	type InsertedChainedReturningWithRelRow = typeof insertedChainedReturningWithRel[0];
	type _I26 = Expect<HasKey<InsertedChainedReturningWithRelRow, "id">>;
	type _I27 = Expect<HasKey<InsertedChainedReturningWithRelRow, "name">>;
	type _I28 = Expect<NotHasKey<InsertedChainedReturningWithRelRow, "active">>;
	type _I29 = Expect<HasKey<InsertedChainedReturningWithRelRow, "sellers">>;

	const insertedNameWithRel = await db
		.insertInto("markets")
		.values({ name: "Test", location: "Helsinki", active: true })
		.returning("name")
		.withRelated("sellers")
		.execute();

	type InsertedNameWithRelRow = typeof insertedNameWithRel[0];
	type _I30 = Expect<HasKey<InsertedNameWithRelRow, "name">>;
	type _I31 = Expect<NotHasKey<InsertedNameWithRelRow, "id">>;
	type _I32 = Expect<NotHasKey<InsertedNameWithRelRow, "active">>;
	type _I33 = Expect<HasKey<InsertedNameWithRelRow, "sellers">>;

	// @ts-expect-error - invalid relation on markets
	db.insertInto("markets").values({ name: "T", location: "H", active: true }).returningAll().withRelated("market");

	// executeTakeFirst returns T | undefined
	const maybeMkt = await db
		.insertInto("markets")
		.values({ name: "T", location: "H", active: true })
		.returningAll()
		.withRelated("sellers")
		.executeTakeFirst();

	type MaybeIns = typeof maybeMkt;
	type _I10 = Expect<Extends<undefined, MaybeIns>>;

	// executeTakeFirstOrThrow returns T
	const defMkt = await db
		.insertInto("markets")
		.values({ name: "T", location: "H", active: true })
		.returningAll()
		.executeTakeFirstOrThrow();

	type DefIns = typeof defMkt;
	type _I11 = Expect<HasKey<DefIns, "id">>;
}

async function testNestedGeneratedColumnTypeMutationValues() {
	db.insertInto("usage_periods").values({
		tenant_id: "tenant-1",
		period_start: "2026-04-01T00:00:00.000Z",
		metric: "chat",
		count: 1,
		updated_at: new Date(),
	});

	// Nullable columns may be omitted or explicitly set on insert.
	db.insertInto("usage_periods").values({
		tenant_id: "tenant-1",
		period_start: new Date(),
		metric: "chat",
		note: null,
	});

	db.updateTable("usage_periods")
		.set({
			period_start: new Date(),
			updated_at: "2026-04-01T00:00:00.000Z",
		})
		.where("tenant_id", "=", "tenant-1");

	// Generated nested ColumnType columns remain optional on insert.
	db.insertInto("usage_periods").values({
		tenant_id: "tenant-1",
		period_start: new Date(),
		metric: "chat",
	});

	db.insertInto("usage_periods")
		.values({
			tenant_id: "tenant-1",
			period_start: new Date(),
			metric: "chat",
		})
		.onConflict((oc) => oc.columns(["tenant_id", "period_start", "metric"]).doUpdateSet({
			count: 2,
			updated_at: new Date(),
		}));

	// @ts-expect-error - required non-generated column is still required
	db.insertInto("usage_periods").values({ tenant_id: "tenant-1", metric: "chat" });

	// @ts-expect-error - invalid value type is still rejected
	db.updateTable("usage_periods").set({ updated_at: 123 });
}

// ============================================================================
// 13. updateTable type safety
// ============================================================================

async function testUpdateTypes() {
	// updateTable accepts valid tables
	const _q = db.updateTable("markets");

	// @ts-expect-error - invalid table
	const _qBad = db.updateTable("nonexistent");

	// set() accepts Updateable columns
	db.updateTable("markets").set({ name: "Updated" });

	// where() type-checks column names
	db.updateTable("markets").set({ name: "Updated" }).where("id", "=", 1);

	// @ts-expect-error - invalid column in where
	db.updateTable("markets").set({ name: "Updated" }).where("nonexistent", "=", 1);

	// returningAll + withRelated
	const updated = await db
		.updateTable("markets")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returningAll()
		.withRelated("sellers")
		.withRelated("tags")
		.execute();

	type UpdRow = typeof updated[0];
	type _U1 = Expect<HasKey<UpdRow, "id">>;
	type _U2 = Expect<HasKey<UpdRow, "name">>;
	type _U3 = Expect<HasKey<UpdRow, "sellers">>;
	type _U4 = Expect<HasKey<UpdRow, "tags">>;
	type _U5 = Expect<Extends<UpdRow["sellers"], Selectable<SellerTable>[]>>;

	const updatedId = await db
		.updateTable("markets")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returning("id")
		.execute();

	type UpdatedIdRow = typeof updatedId[0];
	type _U6 = Expect<HasKey<UpdatedIdRow, "id">>;
	type _U7 = Expect<NotHasKey<UpdatedIdRow, "name">>;

	const updatedIdWithMarket = await db
		.updateTable("sellers")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returning("id")
		.withRelated("market")
		.execute();

	type UpdatedIdWithMarketRow = typeof updatedIdWithMarket[0];
	type _U8 = Expect<HasKey<UpdatedIdWithMarketRow, "id">>;
	type _U9 = Expect<NotHasKey<UpdatedIdWithMarketRow, "market_id">>;
	type _U10 = Expect<HasKey<UpdatedIdWithMarketRow, "market">>;
	type _U11 = Expect<Extends<UpdatedIdWithMarketRow["market"], Selectable<MarketTable> | null>>;

	const updatedIdWithMutatedSellers = await db
		.updateTable("markets")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returning("id")
		.withRelated("sellers", (qb) =>
			qb.update().set({ booth_number: "VIP" }),
		)
		.execute();

	type UpdatedIdWithMutatedSellersRow = typeof updatedIdWithMutatedSellers[0];
	type _U12 = Expect<HasKey<UpdatedIdWithMutatedSellersRow, "id">>;
	type _U13 = Expect<NotHasKey<UpdatedIdWithMutatedSellersRow, "name">>;
	type _U14 = Expect<HasKey<UpdatedIdWithMutatedSellersRow, "sellers">>;
	type _U15 = Expect<Extends<UpdatedIdWithMutatedSellersRow["sellers"], Selectable<SellerTable>[]>>;

	// @ts-expect-error - invalid relation
	db.updateTable("markets").set({ name: "X" }).returningAll().withRelated("nonexistent");

	// @ts-expect-error - invalid returned column
	db.updateTable("markets").set({ name: "X" }).returning("nonexistent");
}

// ============================================================================
// 14. deleteFrom type safety
// ============================================================================

async function testDeleteTypes() {
	// deleteFrom accepts valid tables
	const _q = db.deleteFrom("sellers");

	// @ts-expect-error - invalid table
	const _qBad = db.deleteFrom("nonexistent");

	// returningAll + withRelated
	const deleted = await db
		.deleteFrom("sellers")
		.where("id", "=", 1)
		.returningAll()
		.withRelated("market")
		.withRelated("items")
		.execute();

	type DelRow = typeof deleted[0];
	type _D1 = Expect<HasKey<DelRow, "id">>;
	type _D2 = Expect<HasKey<DelRow, "name">>;
	type _D3 = Expect<HasKey<DelRow, "market">>;
	type _D4 = Expect<HasKey<DelRow, "items">>;
	type _D5 = Expect<Extends<DelRow["market"], Selectable<MarketTable> | null>>;
	type _D6 = Expect<Extends<DelRow["items"], Selectable<ItemTable>[]>>;

	const deletedId = await db
		.deleteFrom("markets")
		.where("id", "=", 1)
		.returning("id")
		.execute();

	type DeletedIdRow = typeof deletedId[0];
	type _D7 = Expect<HasKey<DeletedIdRow, "id">>;
	type _D8 = Expect<NotHasKey<DeletedIdRow, "name">>;

	const deletedIdWithRelations = await db
		.deleteFrom("sellers")
		.where("id", "=", 1)
		.returning("id")
		.withRelated("market")
		.withRelated("items")
		.execute();

	type DeletedIdWithRelationsRow = typeof deletedIdWithRelations[0];
	type _D9 = Expect<HasKey<DeletedIdWithRelationsRow, "id">>;
	type _D10 = Expect<NotHasKey<DeletedIdWithRelationsRow, "market_id">>;
	type _D11 = Expect<HasKey<DeletedIdWithRelationsRow, "market">>;
	type _D12 = Expect<HasKey<DeletedIdWithRelationsRow, "items">>;

	// @ts-expect-error - invalid returned column
	db.deleteFrom("markets").returning("nonexistent");
}

// ============================================================================
// 15. Mutation builder alias works
// ============================================================================

async function testMutationAlias() {
	const result = await db
		.insertInto("markets")
		.values({ name: "T", location: "H", active: true })
		.returningAll()
		.withRelated(
			(b) => b("sellers").as("topSellers"),
			(qb) => qb.limit(5),
		)
		.execute();

	type Row = typeof result[0];
	type _MA1 = Expect<HasKey<Row, "topSellers">>;
	type _MA2 = Expect<NotHasKey<Row, "sellers">>;
	type _MA3 = Expect<Extends<Row["topSellers"], Selectable<SellerTable>[]>>;
}

// ============================================================================
// 16. withRelated mutation type safety (qb.update()/delete()/insert())
// ============================================================================

async function testWithRelatedMutationTypes() {
	// withRelated + update() on toMany: result includes mutated rows as array
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.update().set({ booth_number: "VIP" }),
		)
		.execute();

	type UpdRow = typeof result[0];
	// Parent columns present (markets)
	type _UR2 = Expect<HasKey<UpdRow, "id">>;
	type _UR3 = Expect<HasKey<UpdRow, "name">>;
	// Updated relation present as array
	type _UR4 = Expect<HasKey<UpdRow, "sellers">>;
	type _UR5 = Expect<Extends<UpdRow["sellers"], Selectable<SellerTable>[]>>;

	// withRelated + update() on toOne: result includes updated row as T | null
	const result2 = await db
		.selectFrom("sellers")
		.where("id", "=", 5)
		.withRelated("market", (qb) =>
			qb.update().set({ name: "Updated" }),
		)
		.execute();

	type UpdRow2 = typeof result2[0];
	type _UR6 = Expect<HasKey<UpdRow2, "market">>;
	type _UR7 = Expect<Extends<UpdRow2["market"], Selectable<MarketTable> | null>>;

	// withRelated mutation is chainable with withRelated read
	const result3 = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("tags")
		.withRelated("sellers", (qb) => qb.update().set({ name: "X" }))
		.execute();

	type ChainRow = typeof result3[0];
	type _UR8 = Expect<HasKey<ChainRow, "tags">>;
	type _UR9 = Expect<HasKey<ChainRow, "sellers">>;

	// Modifier callback: update builder has correct types for .set() and .where()
	db.selectFrom("markets").where("id", "=", 1).withRelated("sellers", (qb) =>
		qb.update().set({ booth_number: "A1" }).where("name", "=", "Test"),
	);

	// Builder callback form with alias
	const result4 = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated(
			(b) => b("sellers").as("updatedSellers"),
			(qb) => qb.update().set({ name: "VIP" }),
		)
		.execute();

	type AliasRow = typeof result4[0];
	type _UR10 = Expect<HasKey<AliasRow, "updatedSellers">>;
	type _UR11 = Expect<NotHasKey<AliasRow, "sellers">>;
	type _UR12 = Expect<Extends<AliasRow["updatedSellers"], Selectable<SellerTable>[]>>;
}

// ============================================================================
// 17. mutateRelated type safety (fire-and-forget)
// ============================================================================

async function testMutateRelatedTypes() {
	// mutateRelated does NOT change the output type (fire-and-forget)
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.mutateRelated("sellers", (qb) => qb.delete())
		.execute();

	type DelRow = typeof result[0];
	type _DR1 = Expect<HasKey<DelRow, "id">>;
	type _DR2 = Expect<HasKey<DelRow, "name">>;
	// "sellers" should NOT appear in output (fire-and-forget)
	type _DR3 = Expect<NotHasKey<DelRow, "sellers">>;

	// mutateRelated with .delete().where()
	db.selectFrom("markets").where("id", "=", 1).mutateRelated("sellers", (qb) =>
		qb.delete().where("booth_number", "=", "expired"),
	);

	// mutateRelated is chainable with withRelated
	const result2 = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("tags")
		.mutateRelated("sellers", (qb) => qb.delete())
		.execute();

	type ChainRow = typeof result2[0];
	type _DR4 = Expect<HasKey<ChainRow, "tags">>;
	type _DR5 = Expect<NotHasKey<ChainRow, "sellers">>;

	// @ts-expect-error - invalid relation
	db.selectFrom("markets").mutateRelated("nonexistent", (qb: any) => qb.delete());

	// @ts-expect-error - "market" is not a relation on markets
	db.selectFrom("markets").mutateRelated("market", (qb: any) => qb.delete());

	// Builder callback form
	db.selectFrom("markets")
		.where("id", "=", 1)
		.mutateRelated(
			(b) => b("sellers").on("booth_number", "=", "expired"),
			(qb) => qb.delete(),
		);
}

// ============================================================================
// 18. Projections
// ============================================================================

// Meta with projections
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

const pdb = createOrm(null! as Kysely<Database>, metaWithProjections);

// project() narrows result type to projection columns
async function testProjectNarrows() {
	const result = await pdb.selectFrom("markets").project("summary").execute();
	type Row = typeof result[0];
	type _P1 = Expect<HasKey<Row, "id">>;
	type _P2 = Expect<HasKey<Row, "name">>;
	type _P3 = Expect<NotHasKey<Row, "location">>;
	type _P4 = Expect<NotHasKey<Row, "active">>;
}

// project() + select() accumulates
async function testProjectSelectAccumulates() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary")
		.select(["active"])
		.execute();
	type Row = typeof result[0];
	type _PS1 = Expect<HasKey<Row, "id">>;
	type _PS2 = Expect<HasKey<Row, "name">>;
	type _PS3 = Expect<HasKey<Row, "active">>;
	type _PS4 = Expect<NotHasKey<Row, "location">>;
}

// Invalid projection name rejected
// @ts-expect-error - "nonexistent" is not a projection on markets
const _invalidProj = pdb.selectFrom("markets").project("nonexistent");

// Default projection used when no explicit select
async function testDefaultProjection() {
	const result = await pdb.selectFrom("markets").execute();
	type Row = typeof result[0];
	// markets default: ["id", "name", "location"]
	type _D1 = Expect<HasKey<Row, "id">>;
	type _D2 = Expect<HasKey<Row, "name">>;
	type _D3 = Expect<HasKey<Row, "location">>;
	type _D4 = Expect<NotHasKey<Row, "active">>;
}

// Tables without projections keep Selectable (backward compat)
async function testNoProjBackwardCompat() {
	const result = await pdb.selectFrom("items").execute();
	type Row = typeof result[0];
	// items has no projections — all columns present
	type _BC1 = Expect<HasKey<Row, "id">>;
	type _BC2 = Expect<HasKey<Row, "name">>;
	type _BC3 = Expect<HasKey<Row, "price">>;
	type _BC4 = Expect<HasKey<Row, "seller_id">>;
}

// withRelated uses "relation" projection on target table
async function testWithRelatedRelationProjection() {
	const result = await pdb
		.selectFrom("markets")
		.withRelated("sellers")
		.execute();
	type Row = typeof result[0];
	type _WR1 = Expect<HasKey<Row, "sellers">>;
	// sellers relation projection: ["id", "name", "booth_number"]
	type SellerRow = Row["sellers"][0];
	type _WR2 = Expect<HasKey<SellerRow, "id">>;
	type _WR3 = Expect<HasKey<SellerRow, "name">>;
	type _WR4 = Expect<HasKey<SellerRow, "booth_number">>;
	type _WR5 = Expect<NotHasKey<SellerRow, "market_id">>;
}

// withRelated with variant string narrows type
async function testWithRelatedVariant() {
	const result = await pdb
		.selectFrom("markets")
		.withRelated("sellers", "summary")
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	type _WV1 = Expect<HasKey<SellerRow, "id">>;
	type _WV2 = Expect<HasKey<SellerRow, "name">>;
	type _WV3 = Expect<NotHasKey<SellerRow, "booth_number">>;
	type _WV4 = Expect<NotHasKey<SellerRow, "market_id">>;
}

// withRelated falls back to "default" when no "relation" projection
async function testWithRelatedDefaultFallback() {
	// markets has no "relation" projection, should fall back to "default": ["id", "name", "location"]
	const result = await pdb
		.selectFrom("sellers")
		.withRelated("market")
		.execute();
	type Row = typeof result[0];
	type _WDF1 = Expect<HasKey<Row, "market">>;
	type MarketRow = NonNullable<Row["market"]>;
	type _WDF2 = Expect<HasKey<MarketRow, "id">>;
	type _WDF3 = Expect<HasKey<MarketRow, "name">>;
	type _WDF4 = Expect<HasKey<MarketRow, "location">>;
	type _WDF5 = Expect<NotHasKey<MarketRow, "active">>;
}

// withRelated on table without projections returns full Selectable
async function testWithRelatedNoProjections() {
	const result = await pdb
		.selectFrom("sellers")
		.withRelated("items")
		.execute();
	type Row = typeof result[0];
	type ItemRow = Row["items"][0];
	type _WN1 = Expect<HasKey<ItemRow, "id">>;
	type _WN2 = Expect<HasKey<ItemRow, "name">>;
	type _WN3 = Expect<HasKey<ItemRow, "price">>;
	type _WN4 = Expect<HasKey<ItemRow, "seller_id">>;
}

// select() accumulates: .select(["id"]).select(["name"]) has both
async function testSelectAccumulates() {
	const result = await pdb
		.selectFrom("markets")
		.select(["id"])
		.select(["name"])
		.execute();
	type Row = typeof result[0];
	type _SA1 = Expect<HasKey<Row, "id">>;
	type _SA2 = Expect<HasKey<Row, "name">>;
	type _SA3 = Expect<NotHasKey<Row, "location">>;
}

// Projection meta type-checks column names
const _badProjMeta = {
	markets: {
		projections: {
			// @ts-expect-error - "nonexistent" is not a column of markets
			bad: ["nonexistent"],
		},
	},
} satisfies MetaDB<Database>;

// project() + withRelated + select combined
async function testProjectWithRelatedSelect() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary")
		.withRelated("sellers", "summary")
		.select(["active"])
		.execute();
	type Row = typeof result[0];
	// From project("summary"): id, name
	type _C1 = Expect<HasKey<Row, "id">>;
	type _C2 = Expect<HasKey<Row, "name">>;
	// From select(["active"]): active
	type _C3 = Expect<HasKey<Row, "active">>;
	// From withRelated: sellers
	type _C4 = Expect<HasKey<Row, "sellers">>;
	// Seller from summary: only id, name
	type SellerRow = Row["sellers"][0];
	type _C5 = Expect<HasKey<SellerRow, "id">>;
	type _C6 = Expect<HasKey<SellerRow, "name">>;
	type _C7 = Expect<NotHasKey<SellerRow, "booth_number">>;
}

// Mutation builder withRelated with variant
async function testMutationWithRelatedVariant() {
	const result = await pdb
		.insertInto("markets")
		.values({ name: "T", location: "H", active: true })
		.returningAll()
		.withRelated("sellers", "summary")
		.execute();
	type Row = typeof result[0];
	type _MV1 = Expect<HasKey<Row, "sellers">>;
	type SellerRow = Row["sellers"][0];
	type _MV2 = Expect<HasKey<SellerRow, "id">>;
	type _MV3 = Expect<HasKey<SellerRow, "name">>;
	type _MV4 = Expect<NotHasKey<SellerRow, "booth_number">>;
}

// ============================================================================
// 19. Projection Edge Cases and Type Inference
// ============================================================================

// Multiple project() calls accumulate columns
async function testMultipleProjectAccumulates() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary") // id, name
		.project("default") // id, name, location
		.execute();
	type Row = typeof result[0];
	// Should have union of both projections
	type _MP1 = Expect<HasKey<Row, "id">>;
	type _MP2 = Expect<HasKey<Row, "name">>;
	type _MP3 = Expect<HasKey<Row, "location">>;
	type _MP4 = Expect<NotHasKey<Row, "active">>; // Not in either projection
}

// select() before project() accumulates
async function testSelectThenProject() {
	const result = await pdb
		.selectFrom("markets")
		.select(["active"])
		.project("summary") // id, name
		.execute();
	type Row = typeof result[0];
	type _SP1 = Expect<HasKey<Row, "id">>;
	type _SP2 = Expect<HasKey<Row, "name">>;
	type _SP3 = Expect<HasKey<Row, "active">>;
	type _SP4 = Expect<NotHasKey<Row, "location">>;
}

// project() then select() accumulates
async function testProjectThenSelect() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary") // id, name
		.select(["active"])
		.execute();
	type Row = typeof result[0];
	type _PS1 = Expect<HasKey<Row, "id">>;
	type _PS2 = Expect<HasKey<Row, "name">>;
	type _PS3 = Expect<HasKey<Row, "active">>;
	type _PS4 = Expect<NotHasKey<Row, "location">>;
}

// selectAll() after project() includes all columns
async function testSelectAllAfterProject() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary") // id, name
		.selectAll()
		.execute();
	type Row = typeof result[0];
	// selectAll should include all columns (not narrowed by projection)
	type _SAP1 = Expect<HasKey<Row, "id">>;
	type _SAP2 = Expect<HasKey<Row, "name">>;
	type _SAP3 = Expect<HasKey<Row, "location">>;
	type _SAP4 = Expect<HasKey<Row, "active">>;
	type _SAP5 = Expect<HasKey<Row, "created_at">>;
}

// project() on table without projections is rejected
// @ts-expect-error - items has no projections
const _noProj = pdb.selectFrom("items").project("summary");

// withRelated mutation: .project() in callback only affects runtime SQL, not TypeScript types
// TypeScript type is always determined by RelationResultType (relation → default → full fallback)
async function testMutationWithRelatedProject() {
	const result = await pdb
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb: any) =>
			qb.update().set({ booth_number: "TEST" }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	type _MWP1 = Expect<HasKey<Row, "sellers">>;
	type SellerRow = Row["sellers"][0];
	// TypeScript type uses "relation" projection: ["id", "name", "booth_number"]
	// (the .project("summary") only narrows runtime RETURNING, not TS types)
	type _MWP2 = Expect<HasKey<SellerRow, "id">>;
	type _MWP3 = Expect<HasKey<SellerRow, "name">>;
	type _MWP4 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _MWP5 = Expect<NotHasKey<SellerRow, "market_id">>; // NOT in "relation" projection
	type _MWP6 = Expect<NotHasKey<SellerRow, "created_at">>; // NOT in "relation" projection
}

// INSERT mutation: same type behavior — uses relation projection for types
async function testInsertMutationProject() {
	const result = await pdb
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb: any) =>
			qb.insert().values({ name: "New", booth_number: "Z1" }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// "relation" projection: ["id", "name", "booth_number"]
	type _IMP1 = Expect<HasKey<SellerRow, "id">>;
	type _IMP2 = Expect<HasKey<SellerRow, "name">>;
	type _IMP3 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _IMP4 = Expect<NotHasKey<SellerRow, "market_id">>; // Not in "relation" projection
}

// UPDATE on mutation builder top-level: same — TypeScript uses "relation" projection
async function testUpdateTableProject() {
	const result = await pdb
		.updateTable("markets")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returningAll()
		.withRelated("sellers", (qb: any) =>
			qb.update().set({ name: "VIP" }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// "relation" projection: ["id", "name", "booth_number"]
	type _UTP1 = Expect<HasKey<SellerRow, "id">>;
	type _UTP2 = Expect<HasKey<SellerRow, "name">>;
	type _UTP3 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _UTP4 = Expect<NotHasKey<SellerRow, "market_id">>; // Not in "relation" projection
}

// Nested mutations: inner .withRelated() in (qb: any) callback doesn't affect outer TS type
async function testNestedMutationProject() {
	const result = await pdb
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb: any) =>
			qb
				.update()
				.set({ name: "Parent" })
				.project("summary")
				.withRelated("items", (qb2: any) =>
					qb2.update().set({ price: 100 }),
				),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// TypeScript type: "relation" projection = ["id", "name", "booth_number"]
	// Nested .withRelated("items") inside (qb: any) doesn't appear in the TS type
	type _NMP1 = Expect<HasKey<SellerRow, "id">>;
	type _NMP2 = Expect<HasKey<SellerRow, "name">>;
	type _NMP3 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _NMP4 = Expect<NotHasKey<SellerRow, "market_id">>; // Not in "relation" projection
}

// project() + selectAll() + withRelated combined
async function testProjectSelectAllWithRelated() {
	const result = await pdb
		.selectFrom("markets")
		.project("summary") // id, name
		.selectAll() // all market columns
		.withRelated("sellers", "summary") // sellers summary: id, name
		.execute();
	type Row = typeof result[0];
	// All market columns present (selectAll wins)
	type _PSAW1 = Expect<HasKey<Row, "id">>;
	type _PSAW2 = Expect<HasKey<Row, "name">>;
	type _PSAW3 = Expect<HasKey<Row, "location">>;
	type _PSAW4 = Expect<HasKey<Row, "active">>;
	// Sellers with summary projection
	type SellerRow = Row["sellers"][0];
	type _PSAW5 = Expect<HasKey<SellerRow, "id">>;
	type _PSAW6 = Expect<HasKey<SellerRow, "name">>;
	type _PSAW7 = Expect<NotHasKey<SellerRow, "booth_number">>;
}

// Mutation builder insertInto: sellers type uses "relation" projection
async function testInsertIntoProject() {
	const result = await pdb
		.insertInto("markets")
		.values({ name: "T", location: "H", active: true })
		.returningAll()
		.withRelated("sellers", (qb: any) =>
			qb.insert().values({ name: "S", booth_number: "A1" }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// "relation" projection: ["id", "name", "booth_number"]
	type _IIP1 = Expect<HasKey<SellerRow, "id">>;
	type _IIP2 = Expect<HasKey<SellerRow, "name">>;
	type _IIP3 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _IIP4 = Expect<NotHasKey<SellerRow, "market_id">>; // Not in "relation" projection
}

// Mutation builder deleteFrom: sellers type uses "relation" projection
async function testDeleteFromProject() {
	const result = await pdb
		.deleteFrom("markets")
		.where("id", "=", 1)
		.returningAll()
		.withRelated("sellers", (qb: any) =>
			qb.update().set({ booth_number: "CLOSED" }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// "relation" projection: ["id", "name", "booth_number"]
	type _DFP1 = Expect<HasKey<SellerRow, "id">>;
	type _DFP2 = Expect<HasKey<SellerRow, "name">>;
	type _DFP3 = Expect<HasKey<SellerRow, "booth_number">>; // In "relation" projection
	type _DFP4 = Expect<NotHasKey<SellerRow, "market_id">>; // Not in "relation" projection
}

// Multiple project() on parent + withRelated mutation on child
async function testMultipleProjectMutations() {
	const result = await pdb
		.selectFrom("sellers")
		.project("summary") // id, name
		.project("default") // id, name, market_id
		.withRelated("market", (qb: any) =>
			qb.update().set({ active: false }).project("summary"),
		)
		.execute();
	type Row = typeof result[0];
	// Seller columns: union of summary + default = id, name, market_id
	type _MPM1 = Expect<HasKey<Row, "id">>;
	type _MPM2 = Expect<HasKey<Row, "name">>;
	type _MPM3 = Expect<HasKey<Row, "market_id">>;
	type _MPM4 = Expect<NotHasKey<Row, "booth_number">>; // Not in either projection
	// Market type uses "default" projection (no "relation" on markets): ["id", "name", "location"]
	type MarketRow = NonNullable<Row["market"]>;
	type _MPM5 = Expect<HasKey<MarketRow, "id">>;
	type _MPM6 = Expect<HasKey<MarketRow, "name">>;
	type _MPM7 = Expect<HasKey<MarketRow, "location">>; // In "default" projection
	type _MPM8 = Expect<NotHasKey<MarketRow, "active">>; // Not in "default" projection
}

// ============================================================================
// 20. Nested withRelated Type Inference
// ============================================================================

// Nested toMany inside toMany: sellers have items in the type
async function testNestedToManyType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) => qb.withRelated("items"))
		.execute();
	type Row = typeof result[0];
	type _N1 = Expect<HasKey<Row, "sellers">>;
	type SellerRow = Row["sellers"][0];
	type _N2 = Expect<HasKey<SellerRow, "id">>;
	type _N3 = Expect<HasKey<SellerRow, "name">>;
	// items should be present as a nested relation
	type _N4 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _N5 = Expect<HasKey<ItemRow, "id">>;
	type _N6 = Expect<HasKey<ItemRow, "name">>;
	type _N7 = Expect<HasKey<ItemRow, "price">>;
}

// Nested toOne inside toMany: items have seller in the type
async function testNestedToOneType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.withRelated("items", (qb2) =>
				qb2.withRelated("seller")
			)
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	type _NT1 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _NT2 = Expect<HasKey<ItemRow, "seller">>;
	// seller is toOne so it's T | null
	type SellerBackRef = NonNullable<ItemRow["seller"]>;
	type _NT3 = Expect<HasKey<SellerBackRef, "id">>;
	type _NT4 = Expect<HasKey<SellerBackRef, "name">>;
}

// Triple nesting: markets → sellers → items → seller (with market back-ref)
async function testTripleNestingType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.withRelated("items", (qb2) =>
				qb2.withRelated("seller", (qb3) =>
					qb3.withRelated("market")
				)
			)
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	type ItemRow = SellerRow["items"][0];
	type SellerBackRef = NonNullable<ItemRow["seller"]>;
	// market should be present on the seller back-ref
	type _TN1 = Expect<HasKey<SellerBackRef, "market">>;
	type MarketBackRef = NonNullable<SellerBackRef["market"]>;
	type _TN2 = Expect<HasKey<MarketBackRef, "id">>;
	type _TN3 = Expect<HasKey<MarketBackRef, "name">>;
}

// Nested withRelated with modifier (filter + nested): types still propagate
async function testNestedWithModifierType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.where("name", "=", "Alice").withRelated("items")
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// items should still be in the type even though we also used .where()
	type _NM1 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _NM2 = Expect<HasKey<ItemRow, "id">>;
	type _NM3 = Expect<HasKey<ItemRow, "price">>;
}

// Multiple nested withRelated: seller has both market and items
async function testMultipleNestedType() {
	const result = await db
		.selectFrom("sellers")
		.withRelated("market", (qb) =>
			qb.withRelated("sellers")
		)
		.withRelated("items")
		.execute();
	type Row = typeof result[0];
	// Top-level: both relations present
	type _MN1 = Expect<HasKey<Row, "market">>;
	type _MN2 = Expect<HasKey<Row, "items">>;
	// Nested: market has sellers
	type MarketRow = NonNullable<Row["market"]>;
	type _MN3 = Expect<HasKey<MarketRow, "sellers">>;
	type NestedSeller = MarketRow["sellers"][0];
	type _MN4 = Expect<HasKey<NestedSeller, "id">>;
	type _MN5 = Expect<HasKey<NestedSeller, "name">>;
}

// Mutation callback with (qb: any) => should NOT break the parent type (MergeRelOutput handles `any`)
async function testMutationCallbackNoBreak() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb: any) =>
			qb.update().set({ booth_number: "VIP" })
		)
		.execute();
	type Row = typeof result[0];
	type _MC1 = Expect<HasKey<Row, "sellers">>;
	type SellerRow = Row["sellers"][0];
	type _MC2 = Expect<HasKey<SellerRow, "id">>;
	type _MC3 = Expect<HasKey<SellerRow, "name">>;
}

// Nested withRelated with projections: nested type uses projection fallback
async function testNestedWithProjectionsType() {
	const result = await pdb
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) => qb.withRelated("items"))
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// sellers uses "relation" projection: ["id", "name", "booth_number"]
	type _NP1 = Expect<HasKey<SellerRow, "id">>;
	type _NP2 = Expect<HasKey<SellerRow, "name">>;
	type _NP3 = Expect<HasKey<SellerRow, "booth_number">>;
	type _NP4 = Expect<NotHasKey<SellerRow, "market_id">>;
	// nested items should be present (items has no projections → full Selectable)
	type _NP5 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _NP6 = Expect<HasKey<ItemRow, "id">>;
	type _NP7 = Expect<HasKey<ItemRow, "price">>;
	type _NP8 = Expect<HasKey<ItemRow, "seller_id">>;
}

// ============================================================================
// 21. Mutation builder .withRelated() type propagation
// ============================================================================

// update().set().withRelated("items") — items should appear in the type
async function testUpdateMutationWithRelatedType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.update().set({ booth_number: "TEST" }).withRelated("items"),
		)
		.execute();
	type Row = typeof result[0];
	type _UM1 = Expect<HasKey<Row, "sellers">>;
	type SellerRow = Row["sellers"][0];
	type _UM2 = Expect<HasKey<SellerRow, "id">>;
	type _UM3 = Expect<HasKey<SellerRow, "name">>;
	// items should be present from the mutation builder's .withRelated()
	type _UM4 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _UM5 = Expect<HasKey<ItemRow, "id">>;
	type _UM6 = Expect<HasKey<ItemRow, "name">>;
	type _UM7 = Expect<HasKey<ItemRow, "price">>;
}

// insert().values().withRelated("items") — items should appear in the type
async function testInsertMutationWithRelatedType() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.insert().values({ name: "New", booth_number: "Z1" }).withRelated("items"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	type _IM1 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _IM2 = Expect<HasKey<ItemRow, "id">>;
	type _IM3 = Expect<HasKey<ItemRow, "price">>;
}

// update().withRelated() on OrmReturningBuilder
async function testReturningBuilderMutationWithRelatedType() {
	const result = await db
		.updateTable("markets")
		.set({ name: "Updated" })
		.where("id", "=", 1)
		.returningAll()
		.withRelated("sellers", (qb) =>
			qb.update().set({ booth_number: "VIP" }).withRelated("items"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	type _RB1 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _RB2 = Expect<HasKey<ItemRow, "id">>;
	type _RB3 = Expect<HasKey<ItemRow, "price">>;
}

// (qb: any) => still works — MergeRelOutput handles any, items NOT in type
async function testMutationWithAnyCallbackStillWorks() {
	const result = await db
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb: any) =>
			qb.update().set({ booth_number: "TEST" }).withRelated("items"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// With (qb: any), RO is any, MergeRelOutput returns Base unchanged
	type _AC1 = Expect<HasKey<SellerRow, "id">>;
	type _AC2 = Expect<HasKey<SellerRow, "name">>;
}

// Multiple .withRelated() on mutation builder accumulates types
async function testMutationMultipleWithRelated() {
	const result = await db
		.selectFrom("sellers")
		.where("id", "=", 1)
		.withRelated("items", (qb) =>
			qb.update().set({ price: 0 }).withRelated("seller"),
		)
		.execute();
	type Row = typeof result[0];
	type ItemRow = Row["items"][0];
	type _MM1 = Expect<HasKey<ItemRow, "seller">>;
	// seller is toOne, so T | null
	type SellerRef = NonNullable<ItemRow["seller"]>;
	type _MM2 = Expect<HasKey<SellerRef, "id">>;
	type _MM3 = Expect<HasKey<SellerRef, "name">>;
}

// With projections: mutation builder .withRelated() still propagates
async function testMutationWithRelatedProjections() {
	const result = await pdb
		.selectFrom("markets")
		.where("id", "=", 1)
		.withRelated("sellers", (qb) =>
			qb.update().set({ booth_number: "TEST" }).withRelated("items"),
		)
		.execute();
	type Row = typeof result[0];
	type SellerRow = Row["sellers"][0];
	// sellers uses "relation" projection: ["id", "name", "booth_number"]
	type _MP1 = Expect<HasKey<SellerRow, "id">>;
	type _MP2 = Expect<HasKey<SellerRow, "name">>;
	type _MP3 = Expect<HasKey<SellerRow, "booth_number">>;
	type _MP4 = Expect<NotHasKey<SellerRow, "market_id">>;
	// items should be present (items has no projections → full Selectable)
	type _MP5 = Expect<HasKey<SellerRow, "items">>;
	type ItemRow = SellerRow["items"][0];
	type _MP6 = Expect<HasKey<ItemRow, "id">>;
	type _MP7 = Expect<HasKey<ItemRow, "price">>;
}

// ============================================================================
// Done — if tsc --noEmit passes with 0 errors, all type tests are green.
// ============================================================================
