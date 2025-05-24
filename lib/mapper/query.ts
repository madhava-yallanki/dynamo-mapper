import { EntityConfig, PrimaryKey } from '../entities/base.js';
import { buildKey, Expression, getKeyConfig, nameAlias, DocClientKey } from './utils.js';

export function buildProjection(attributes?: string[]): Partial<Pick<Expression, 'text' | 'nameAliases'>> {
  if (!attributes || attributes.length === 0) {
    return {};
  }

  const text = attributes.map((attribute) => nameAlias(attribute)).join(',');
  const nameAliases: Expression['nameAliases'] = {};
  attributes.forEach((attribute) => (nameAliases[nameAlias(attribute)] = attribute));
  return { text, nameAliases };
}

export type ExclusiveStartKeyArgs = {
  indexName?: string;
  exclusiveStartKey?: { tablePk: PrimaryKey; indexPk?: PrimaryKey };
};

export function buildExclusiveStartKey(
  entityConfig: EntityConfig,
  args?: ExclusiveStartKeyArgs,
): DocClientKey | undefined {
  if (!args?.exclusiveStartKey) {
    return undefined;
  }

  const tableKey = buildKey(entityConfig.table, args.exclusiveStartKey.tablePk);
  if (!args.indexName || !args.exclusiveStartKey.indexPk) {
    return tableKey;
  }

  const indexKeyConfig = getKeyConfig(entityConfig, args.indexName);
  const indexKey = buildKey(indexKeyConfig, args.exclusiveStartKey.indexPk);
  return { ...tableKey, ...indexKey };
}
