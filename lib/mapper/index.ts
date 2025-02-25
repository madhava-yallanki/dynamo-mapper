import { DynamoDB, TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument, type PutCommandInput, type TransactWriteCommandInput } from '@aws-sdk/lib-dynamodb';
import { VersionMismatchError, NotFoundError } from '../errors/index.js';
import { Logger } from '../logger/index.js';
import { ConstructorItem, EntityBase, EntityConfig, PrimaryKey, PrimaryKeyNames, Table } from '../entities/base.js';
import { FilterBuilder } from './filters.js';
import { KeyConditionBuilder } from './keyCondition.js';
import { UpdateExpression } from './updateExpression.js';
import { Expression, Filter, nameAlias, QueryParams, UpdateOptions } from './utils.js';

const logger = new Logger({ module: import.meta.url });

type GetOptions = { consistentRead?: boolean };

type QueryOptions<E, PA> = {
  indexName?: string;
  filters?: Filter<E>[];
  scanIndexForward?: boolean;
  limit?: number;
  attributes?: PA;
};

export type QueryResult<E> = { items: E[]; lastEvaluatedKey: DocClientKey | undefined };

type PutOptions = {
  skipVersionCheck?: boolean;
};

type PutConditionArgs = {
  currentVersion: number;
  skipVersionCheck?: boolean;
};

type UpsertCondition = {
  expression: string;
  attributeValues: Record<string, string | number>;
};

type DocClientKey = Record<string, string | number>;

type EntityTarget<E extends EntityBase> = typeof EntityBase & {
  new (item: ConstructorItem<E>): E;
};

type DynamoMapperArgs = {
  region?: string;
};

export class DynamoMapper {
  private readonly client: DynamoDBDocument;
  private static _cachedClient: DynamoDBDocument | undefined;

  constructor(args?: DynamoMapperArgs) {
    if (!DynamoMapper._cachedClient) {
      DynamoMapper._cachedClient = DynamoDBDocument.from(new DynamoDB({ region: args?.region }), {
        marshallOptions: {
          convertClassInstanceToMap: true,
          removeUndefinedValues: true,
        },
      });
      logger.info('Created and cached Dynamo DB document client');
    }

    this.client = DynamoMapper._cachedClient;
  }

  async getItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    options?: GetOptions,
  ): Promise<E | undefined> {
    const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
    logger.debug({ primaryKey, docClientKey }, 'Generated Dynamo DB key');

    const response = await this.client.get({
      TableName: entityClass.entityConfig.table.tableName,
      Key: docClientKey,
      ConsistentRead: options?.consistentRead,
    });

    if (!response.Item) {
      return undefined;
    }

    return new entityClass(response.Item as E) as E;
  }

  async putItem<E extends EntityBase>(item: E, options?: PutOptions): Promise<E> {
    const input = this.buildPutItemInput(item, options);

    try {
      await this.client.put(input);
      return item;
    } catch (error) {
      logger.error({ error }, 'Error in put item request');
      if (error instanceof Error && error.name === 'ConditionalCheckFailedException') {
        throw new VersionMismatchError();
      }

      throw error;
    }
  }

  async transactPut<T extends EntityBase[]>(items: T): Promise<T> {
    if (items.length === 0) {
      throw new Error('Items to save cannot be empty');
    }

    if (items.length === 1) {
      await this.putItem(items[0]);
      return items;
    }

    try {
      for (const { TransactItems } of this.buildTransactBatches(items)) {
        const response = await this.client.transactWrite({ TransactItems });
        logger.info({ response }, `Put transaction completed for ${TransactItems?.length} items`);
      }

      return items;
    } catch (error) {
      logger.error({ error }, 'Error in transact put items request');
      if (
        error instanceof TransactionCanceledException &&
        error.CancellationReasons?.find((reason) => reason.Code === 'ConditionalCheckFailed')
      ) {
        throw new VersionMismatchError();
      }

      throw error;
    }
  }

  private buildTransactBatches<T extends EntityBase[]>(items: T): TransactWriteCommandInput[] {
    if (items.length <= 100) {
      return [{ TransactItems: items.map((item) => ({ Put: this.buildPutItemInput(item) })) }];
    }

    const chunkSize = 100;
    const batches: TransactWriteCommandInput[] = [];
    logger.warn({ items: items.length, chunkSize }, 'Chunking transact operation of unsupported length.');
    for (let index = 0; index < items.length; index += chunkSize) {
      const chunk = items.slice(index, index + chunkSize);
      batches.push({ TransactItems: chunk.map((item) => ({ Put: this.buildPutItemInput(item) })) });
    }
    return batches;
  }

  private buildPutItemInput<E extends EntityBase>(item: E, options?: PutOptions): PutCommandInput {
    const entityConfig = item.getEntityConfig();
    const docClientKey = buildKey(entityConfig.table, entityConfig.keyGenerator(item));
    Object.assign(item, docClientKey);
    const currentVersion = item.versionNumber || 0;
    item.versionNumber = currentVersion + 1;

    const putCondition = this.buildPutCondition({ currentVersion, skipVersionCheck: options?.skipVersionCheck });
    logger.debug({ docClientKey, putCondition }, 'Assigned keys and version');

    return {
      TableName: entityConfig.table.tableName,
      Item: item,
      ConditionExpression: putCondition?.expression,
      ExpressionAttributeValues: putCondition?.attributeValues,
      ReturnValues: 'NONE',
    };
  }

  private buildPutCondition(args: PutConditionArgs): UpsertCondition | undefined {
    if (args.skipVersionCheck) {
      return undefined;
    }

    return {
      expression: 'versionNumber = :currentVersion OR attribute_not_exists(versionNumber)',
      attributeValues: { ':currentVersion': args.currentVersion },
    };
  }

  async updateItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    item: Partial<Omit<E, 'versionNumber'>>,
    options?: UpdateOptions<E>,
  ): Promise<E> {
    const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
    logger.info({ primaryKey, docClientKey }, 'Generated Dynamo DB key');
    const { text, nameAliases, valueAliases } = new UpdateExpression({ item, options }).build();
    logger.info({ text, nameAliases, valueAliases }, 'Constructed update expression');

    try {
      const response = await this.client.update({
        TableName: entityClass.entityConfig.table.tableName,
        Key: docClientKey,
        UpdateExpression: text,
        ExpressionAttributeNames: nameAliases,
        ExpressionAttributeValues: valueAliases,
        ReturnValues: 'ALL_NEW',
      });

      return new entityClass(response.Attributes as E) as E;
    } catch (error) {
      logger.error({ error }, 'Error in update item request');
      if (error instanceof Error && error.name === 'ValidationException') {
        throw new NotFoundError();
      }

      throw error;
    }
  }

  async deleteItem<E extends EntityBase>(entityClass: EntityTarget<E>, primaryKey: PrimaryKey): Promise<E | undefined> {
    const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
    logger.debug({ primaryKey, docClientKey }, 'Generated Dynamo DB key');

    const response = await this.client.delete({
      TableName: entityClass.entityConfig.table.tableName,
      Key: docClientKey,
      ReturnValues: 'ALL_OLD',
    });

    if (!response.Attributes) {
      return undefined;
    }

    return new entityClass(response.Attributes as E) as E;
  }

  async query<E extends EntityBase, PA extends Array<keyof E> | undefined = undefined>(
    entityClass: EntityTarget<E>,
    queryParams: QueryParams,
    options?: QueryOptions<E, PA>,
  ): Promise<QueryResult<PA extends Array<keyof E> ? Partial<E> : E>> {
    const primaryKeyNames = getPrimaryKeyNames(entityClass.entityConfig, options?.indexName);
    const keyCondition = new KeyConditionBuilder({ primaryKeyNames, queryParams }).build();
    const projection = buildProjection(options?.attributes as string[]);
    const filter = new FilterBuilder<E>({ filters: options?.filters }).build();
    logger.debug({ primaryKeyNames, keyCondition, projection, filter }, 'Derived query options');

    const response = await this.client.query({
      TableName: entityClass.entityConfig.table.tableName,
      IndexName: options?.indexName,
      KeyConditionExpression: keyCondition.text,
      FilterExpression: filter.text,
      ExpressionAttributeValues: { ...keyCondition.valueAliases, ...filter.valueAliases },
      ProjectionExpression: projection.text,
      ExpressionAttributeNames: { ...keyCondition.nameAliases, ...projection.nameAliases, ...filter.nameAliases },
      ScanIndexForward: options?.scanIndexForward,
      Limit: options?.limit,
    });

    const items = (response.Items || []).map((item) => new entityClass(item as E) as E);
    return { items: items, lastEvaluatedKey: response.LastEvaluatedKey };
  }
}

const getPrimaryKeyNames = (entityConfig: EntityConfig, indexName?: string): PrimaryKeyNames => {
  if (!indexName) {
    return entityConfig.table;
  }

  if (!entityConfig.indexes || !entityConfig.indexes[indexName]) {
    throw new Error(`Index ${indexName} is not defined on the Entity`);
  }

  return entityConfig.indexes[indexName];
};

const buildKey = (table: Table, primaryKey: PrimaryKey): DocClientKey => {
  const key: DocClientKey = { [table.partitionKeyName]: primaryKey.partitionKey };
  if (table.sortKeyName && primaryKey.sortKey !== undefined) {
    key[table.sortKeyName] = primaryKey.sortKey;
  }

  return key;
};

function buildProjection(attributes?: string[]): Partial<Pick<Expression, 'text' | 'nameAliases'>> {
  if (!attributes || attributes.length === 0) {
    return {};
  }

  const text = attributes.map((attribute) => nameAlias(attribute)).join(',');
  const nameAliases: Expression['nameAliases'] = {};
  attributes.forEach((attribute) => (nameAliases[nameAlias(attribute)] = attribute));
  return { text, nameAliases };
}
