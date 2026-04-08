import { type ComparisonOperatorExpression } from "kysely";
import type { RelationNames, RelationTarget } from "./types.js";

export interface OnCondition {
	kind: "value" | "ref";
	lhs: string;
	op: ComparisonOperatorExpression;
	rhs: any;
}

export interface RelBuilderConfig {
	name: string;
	alias: string;
	joinType: "left" | "inner";
	onConditions: OnCondition[];
}

export class RelBuilder<
	DB extends Record<string, any>,
	TB extends keyof DB & string,
	R extends string,
	Target extends keyof DB & string,
	A extends string = R,
> {
	readonly _config: RelBuilderConfig;
	constructor(config: RelBuilderConfig) { this._config = config; }

	as<NewA extends string>(alias: NewA): RelBuilder<DB, TB, R, Target, NewA> {
		return new RelBuilder({ ...this._config, alias });
	}
	inner(): RelBuilder<DB, TB, R, Target, A> {
		return new RelBuilder({ ...this._config, joinType: "inner" });
	}
	on<C extends keyof DB[Target] & string>(
		column: C, op: ComparisonOperatorExpression, value: any,
	): RelBuilder<DB, TB, R, Target, A> {
		return new RelBuilder({
			...this._config,
			onConditions: [...this._config.onConditions, { kind: "value", lhs: column, op, rhs: value }],
		});
	}
	onRef<C extends keyof DB[Target] & string, Ref extends keyof DB[TB] & string>(
		column: C, op: ComparisonOperatorExpression, ref: Ref,
	): RelBuilder<DB, TB, R, Target, A> {
		return new RelBuilder({
			...this._config,
			onConditions: [...this._config.onConditions, { kind: "ref", lhs: column, op, rhs: ref }],
		});
	}
}

export type RelBuilderFactory<DB extends Record<string, any>, M, TB extends string> =
	<R extends RelationNames<M, TB>>(name: R) => RelBuilder<DB, TB, R, RelationTarget<M, TB, R>>;

export function createRelBuilderFactory<
	DB extends Record<string, any>, M, TB extends string,
>(): RelBuilderFactory<DB, M, TB> {
	return <R extends RelationNames<M, TB>>(name: R) =>
		new RelBuilder<DB, TB, R, RelationTarget<M, TB, R>>({
			name: name as string, alias: name as string, joinType: "left", onConditions: [],
		});
}
