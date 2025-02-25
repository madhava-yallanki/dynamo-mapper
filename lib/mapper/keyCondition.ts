import { KeyConfig } from '../entities/base.js';
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
 * All the attributes names are prefixed with # to prevent colliding with DynamoDB reserved keywords.
 * All the attributes values are prefixed with : to prevent colliding with DynamoDB reserved keywords.
 */
export class KeyConditionBuilder {
  private readonly keyConfig: KeyConfig;
  private readonly queryParams: QueryParams;
  private readonly expression: Expression;

  constructor({ keyConfig, queryParams }: { keyConfig: KeyConfig; queryParams: QueryParams }) {
    this.keyConfig = keyConfig;
    this.queryParams = queryParams;
    this.expression = { text: '', nameAliases: {}, valueAliases: {} };
  }

  build(): Expression {
    const { pkName } = this.keyConfig;
    this.expression.text = `${nameAlias(pkName)} = ${valueAlias(pkName)}`;
    this.expression.nameAliases[nameAlias(pkName)] = pkName;
    this.expression.valueAliases[valueAlias(pkName)] = this.queryParams.partitionKey;

    if (!this.queryParams.operator) {
      return this.expression;
    }

    if (this.queryParams.operator === Operator.Between) {
      return this.applyBetweenOperator(this.queryParams);
    }

    return this.applyOperator(this.queryParams);
  }

  private applyOperator({ operator, sortKey }: QueryOperatorOptions): Expression {
    const { skName } = this.keyConfig;
    if (!skName) {
      throw new Error(`Sort key is not present for ${operator} operation`);
    }

    this.expression.nameAliases[nameAlias(skName)] = skName;
    this.expression.valueAliases[valueAlias(skName)] = sortKey;
    const text = ExpressionText.for({ operator, attributeName: skName });
    this.expression.text = `${this.expression.text} and ${text}`;
    return this.expression;
  }

  private applyBetweenOperator({ sortKeyValues }: QueryBetweenOptions): Expression {
    const { skName } = this.keyConfig;
    if (!skName || !sortKeyValues || sortKeyValues.length !== 2) {
      throw new Error('Sort key/Range is not present for between operation');
    }

    this.expression.nameAliases[nameAlias(skName)] = skName;
    this.expression.valueAliases[fromValueAlias(skName)] = sortKeyValues[0];
    this.expression.valueAliases[toValueAlias(skName)] = sortKeyValues[1];
    const text = ExpressionText.for({ operator: Operator.Between, attributeName: skName });
    this.expression.text = `${this.expression.text} and ${text}`;

    return this.expression;
  }
}
