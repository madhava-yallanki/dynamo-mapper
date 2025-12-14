import { EntityConfig, PrimaryKey } from '../entities/base.js';
import { buildKey, getKeyConfig, DocClientKey } from './utils.js';

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
