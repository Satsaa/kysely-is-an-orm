# `kysely-is-an-orm` Internal Notes

This package extends Kysely without replacing Kysely's query model.

## Invariants

- Preserve native Kysely behavior unless relation loading or projections require an explicit extension.
- Keep metadata as the single source of truth for relation structure and projection variants.
- Prefer compile-time guarantees over runtime normalization.
- Generated SQL must stay inspectable and predictable.

## Structure

- [`src/meta.ts`](./src/meta.ts) defines relation metadata types.
- [`src/orm.ts`](./src/orm.ts) augments a Kysely instance with ORM-aware builders.
- [`src/select-builder.ts`](./src/select-builder.ts) handles relation loading and projections for selects.
- [`src/mutation-builders.ts`](./src/mutation-builders.ts) and [`src/returning-builder.ts`](./src/returning-builder.ts) extend write queries.
- [`src/relation-builder.ts`](./src/relation-builder.ts) and [`src/relation-mutation-builder.ts`](./src/relation-mutation-builder.ts) drive nested relation behavior.

## Rules

- Do not add adapter layers to paper over bad metadata. Tighten types instead.
- Do not introduce hidden runtime conventions beyond what `MetaDB` declares.
- Keep type tests exhaustive when relation inference or projection behavior changes.
