import { ExpressionText } from './expression.js';
import { Expression, Filter, fromValueAlias, nameAlias, Operator, toValueAlias, valueAlias } from './utils.js';

export class FilterBuilder<E> {
  private readonly filters: Filter<E>[] | undefined;
  private readonly expression: Expression;
  private readonly textSegments: string[];

  constructor({ filters }: { filters?: Filter<E>[] }) {
    this.filters = filters;
    this.expression = { nameAliases: {}, valueAliases: {} };
    this.textSegments = [];
  }

  build(): Expression {
    if (!this.filters || this.filters.length === 0) {
      return this.expression;
    }

    this.filters.forEach((filter) => this.processFilter(filter));
    this.expression.text = this.filters.length === 1 ? this.textSegments.join('') : this.textSegments.join(' and ');

    return this.expression;
  }

  private processFilter(filter: Filter<E>): void {
    const attributeName = filter.attributeName as string;
    this.expression.nameAliases[nameAlias(attributeName)] = attributeName;

    if (filter.operator === Operator.Between) {
      this.expression.valueAliases[fromValueAlias(attributeName)] = filter.attributeValues[0];
      this.expression.valueAliases[toValueAlias(attributeName)] = filter.attributeValues[1];
    } else if (filter.operator !== Operator.Exists) {
      this.expression.valueAliases[valueAlias(attributeName)] = filter.attributeValue;
    }

    const textSegment = ExpressionText.for({ operator: filter.operator, attributeName: attributeName });
    this.textSegments.push(textSegment);
  }
}
