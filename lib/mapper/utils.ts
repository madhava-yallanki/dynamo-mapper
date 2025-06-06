import { EntityConfig, KeyConfig, PrimaryKey } from '../entities/base.js';

export const nameAlias = (param: string): string => `#${param}`;
export const valueAlias = (param: string): string => `:${param}`;
export const fromValueAlias = (param: string): string => `:${param}From`;
export const toValueAlias = (param: string): string => `:${param}To`;

export enum Operator {
  EQ = '=',
  LE = '<=',
  LT = '<',
  GE = '>=',
  GT = '>',
  BeginsWith = 'begins_with',
  Between = 'between',
  Exists = 'attribute_exists',
}

type AttributeValue = string | number;

export type DocClientKey = Record<string, string | number>;

export type QueryOperatorOptions = {
  operator: Exclude<Operator, Operator.Between | Operator.Exists>;
  sortKey: AttributeValue;
};

export type QueryBetweenOptions = {
  operator: Operator.Between;
  sortKeyValues: [AttributeValue, AttributeValue];
};

export type QueryParams = { partitionKey: AttributeValue } & (
  | { sortKey?: never; operator?: never }
  | QueryOperatorOptions
  | QueryBetweenOptions
);

export type Expression = {
  text?: string;
  nameAliases: Record<string, string>;
  valueAliases: Record<string, string | number | unknown[]>;
};

export type Filter<E> = { attributeName: keyof E } & (
  | { operator: Exclude<Operator, Operator.Between | Operator.Exists>; attributeValue: AttributeValue }
  | { operator: Operator.Between; attributeValues: [AttributeValue, AttributeValue] }
  | { operator: Operator.Exists }
);

export type UpdateOptions<E> = {
  listAppendFields?: Partial<Record<keyof E, unknown[]>>;
  incrementFields?: Partial<Record<keyof E, number>>;
};

export function getKeyConfig(entityConfig: EntityConfig, indexName?: string): KeyConfig {
  if (!indexName) {
    return entityConfig.table;
  }

  if (!entityConfig.indexes || !entityConfig.indexes[indexName]) {
    throw new Error(`Index ${indexName} is not defined on the Entity`);
  }

  return entityConfig.indexes[indexName];
}

export function buildKey(table: KeyConfig, primaryKey: PrimaryKey): DocClientKey {
  const key: DocClientKey = { [table.pkName]: primaryKey.partitionKey };
  if (table.skName && primaryKey.sortKey !== undefined) {
    key[table.skName] = primaryKey.sortKey;
  }

  return key;
}
