import { fromValueAlias, nameAlias, Operator, toValueAlias, valueAlias } from './utils.js';

type ExpressionTextOptions = {
  operator: Operator;
  attributeName: string;
};

export class ExpressionText {
  static for({ operator, attributeName }: ExpressionTextOptions): string {
    if (operator === Operator.Between) {
      return ExpressionText.between(attributeName);
    }

    if (operator === Operator.BeginsWith) {
      return ExpressionText.beginsWith(attributeName);
    }

    if (operator === Operator.Exists) {
      return ExpressionText.exists(attributeName);
    }

    return `${nameAlias(attributeName)} ${operator} ${valueAlias(attributeName)}`;
  }

  private static beginsWith(name: string): string {
    return `${Operator.BeginsWith}(${nameAlias(name)}, ${valueAlias(name)})`;
  }

  private static exists(name: string): string {
    return `${Operator.Exists}(${nameAlias(name)})`;
  }

  private static between(name: string): string {
    return `${nameAlias(name)} ${Operator.Between} ${fromValueAlias(name)} and ${toValueAlias(name)}`;
  }
}
