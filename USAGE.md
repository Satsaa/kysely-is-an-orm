# `kysely-is-an-orm` Usage

`kysely-is-an-orm` extends Kysely with typed relations, relation mutations, and projections while keeping the normal Kysely query surface.

## Setup

```ts
import { Kysely, PostgresDialect, type Generated } from "kysely";
import { createOrm, type MetaDB } from "kysely-is-an-orm";

interface Database {
	markets: {
		id: Generated<number>;
		name: string;
		location: string;
		active: boolean;
	};
	sellers: {
		id: Generated<number>;
		market_id: number;
		name: string;
	};
}

const meta = {
	markets: {
		relations: {
			sellers: { model: "sellers", type: "many", from: "id", to: "market_id" },
		},
	},
	sellers: {
		relations: {
			market: { model: "markets", type: "one", from: "market_id", to: "id" },
		},
	},
} as const satisfies MetaDB<Database>;

const rawDb = new Kysely<Database>({ dialect: new PostgresDialect({ pool }) });
const db = createOrm(rawDb, meta);
```

## Load relations

```ts
const market = await db
	.selectFrom("markets")
	.where("id", "=", 1)
	.withRelated("sellers")
	.executeTakeFirstOrThrow();

market.sellers;
```

Nested relations use the related query builder:

```ts
const market = await db
	.selectFrom("markets")
	.withRelated("sellers", (qb) => qb.withRelated("market"))
	.executeTakeFirstOrThrow();
```

## Mutate related rows

```ts
const updated = await db
	.selectFrom("markets")
	.where("id", "=", 1)
	.withRelated("sellers", (qb) =>
		qb.update().set({ name: "Updated seller" }),
	)
	.executeTakeFirstOrThrow();
```

Use `.mutateRelated()` when the related mutation does not need to be returned.

## Projections

Define projection variants in metadata, then select them with `.project()` or relation-level projection arguments.

```ts
const summaries = await db
	.selectFrom("markets")
	.project("summary")
	.execute();
```
