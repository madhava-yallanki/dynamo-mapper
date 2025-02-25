import { PrimaryKeyNames } from '../entities/base.js';
import { ExpressionText } from './expression.js';
import {
  Expression,
  fromValueAlias,
  nameAlias,
  Operator,
  QueryBetweenOptions,
  QueryOperatorOptions,
  QueryParams,
  toValueAlias,
  valueAlias,
} from './utils.js';

/**
 * Class to build the Query expression, attribute names, and attribute values.
 * All the attributes names are prefixed with # to prevent colliding with DynamoDB reserved keywords.
 * All the attributes values are prefixed with : to prevent colliding with DynamoDB reserved keywords.
 *
 * @param primaryKeyNames Names defined on the entity for partition and sort keys
 * @param queryParams Key to use to query the items.
 */
export class KeyConditionBuilder {
  private readonly primaryKeyNames: PrimaryKeyNames;
  private readonly queryParams: QueryParams;
  private readonly expression: Expression;

  constructor({ primaryKeyNames, queryParams }: { primaryKeyNames: PrimaryKeyNames; queryParams: QueryParams }) {
    this.primaryKeyNames = primaryKeyNames;
    this.queryParams = queryParams;
    this.expression = { text: '', nameAliases: {}, valueAliases: {} };
  }

  build(): Expression {
    const { partitionKeyName } = this.primaryKeyNames;
    this.expression.text = `${nameAlias(partitionKeyName)} = ${valueAlias(partitionKeyName)}`;
    this.expression.nameAliases[nameAlias(partitionKeyName)] = partitionKeyName;
    this.expression.valueAliases[valueAlias(partitionKeyName)] = this.queryParams.partitionKey;

    if (!this.queryParams.operator) {
      return this.expression;
    }

    if (this.queryParams.operator === Operator.Between) {
      return this.applyBetweenOperator(this.queryParams);
    }

    return this.applyOperator(this.queryParams);
  }

  private applyOperator({ operator, sortKey }: QueryOperatorOptions): Expression {
    const { sortKeyName } = this.primaryKeyNames;
    if (!sortKeyName) {
      throw new Error(`Sort key is not present for ${operator} operation`);
    }

    this.expression.nameAliases[nameAlias(sortKeyName)] = sortKeyName;
    this.expression.valueAliases[valueAlias(sortKeyName)] = sortKey;
    const text = ExpressionText.for({ operator, attributeName: sortKeyName });
    this.expression.text = `${this.expression.text} and ${text}`;
    return this.expression;
  }

  private applyBetweenOperator({ sortKeyValues }: QueryBetweenOptions): Expression {
    const { sortKeyName } = this.primaryKeyNames;
    if (!sortKeyName || !sortKeyValues || sortKeyValues.length !== 2) {
      throw new Error('Sort key/Range is not present for between operation');
    }

    this.expression.nameAliases[nameAlias(sortKeyName)] = sortKeyName;
    this.expression.valueAliases[fromValueAlias(sortKeyName)] = sortKeyValues[0];
    this.expression.valueAliases[toValueAlias(sortKeyName)] = sortKeyValues[1];
    const text = ExpressionText.for({ operator: Operator.Between, attributeName: sortKeyName });
    this.expression.text = `${this.expression.text} and ${text}`;

    return this.expression;
  }
}
