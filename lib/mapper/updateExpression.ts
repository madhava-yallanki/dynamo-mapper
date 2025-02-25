import { EntityBase } from '../entities/base.js';
import { Expression, nameAlias, valueAlias, UpdateOptions } from './utils.js';

type UpdateExpressionArgs<E> = {
  item: Partial<E>;
  options?: UpdateOptions<E>;
};

export class UpdateExpression<E extends EntityBase> {
  private readonly expression: Expression;
  private readonly item: Partial<E>;
  private readonly options: UpdateOptions<E> | undefined;
  private readonly textElements: string[];

  constructor({ item, options }: UpdateExpressionArgs<E>) {
    this.item = item;
    this.options = options;
    this.expression = { text: '', nameAliases: {}, valueAliases: {} };
    this.textElements = [`versionNumber = versionNumber + ${valueAlias('versionInc')}`];
    this.expression.valueAliases[valueAlias('versionInc')] = 1;
  }

  build(): Expression {
    this.setExpressionForItem();
    this.setListAppendExpression();
    this.setIncrementsExpression();

    this.expression.text = `SET ${this.textElements.join(',')}`;
    return this.expression;
  }

  private setExpressionForItem(): void {
    for (const [attributeName, attributeValue] of Object.entries(this.item)) {
      this.textElements.push(`${nameAlias(attributeName)} = ${valueAlias(attributeName)}`);
      this.expression.nameAliases[nameAlias(attributeName)] = attributeName;
      this.expression.valueAliases[valueAlias(attributeName)] = (attributeValue ?? null) as never;
    }
  }

  private setListAppendExpression(): void {
    if (!this.options?.listAppendFields) {
      return;
    }

    const emptyValueAlias = valueAlias('emptyList');
    for (const [attributeName, attributeValue] of Object.entries(this.options.listAppendFields)) {
      this.textElements.push(
        `${nameAlias(attributeName)} = list_append(if_not_exists(${nameAlias(attributeName)}, ${emptyValueAlias}), ${valueAlias(attributeName)})`,
      );
      this.expression.nameAliases[nameAlias(attributeName)] = attributeName;
      this.expression.valueAliases[emptyValueAlias] = [];
      this.expression.valueAliases[valueAlias(attributeName)] = attributeValue as never;
    }
  }

  private setIncrementsExpression(): void {
    if (!this.options?.incrementFields) {
      return;
    }

    const zeroValueAlias = valueAlias('zero');
    for (const [attributeName, increment] of Object.entries(this.options.incrementFields)) {
      const incValueAlias = valueAlias(`${attributeName}Inc`);
      this.textElements.push(
        `${nameAlias(attributeName)} = if_not_exists(${nameAlias(attributeName)}, ${zeroValueAlias}) + ${incValueAlias}`,
      );
      this.expression.nameAliases[nameAlias(attributeName)] = attributeName;
      this.expression.valueAliases[zeroValueAlias] = 0;
      this.expression.valueAliases[incValueAlias] = increment as number;
    }
  }
}
