import { DynamoDB, TransactionCanceledException, ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument, type PutCommandInput, type DeleteCommandInput } from '@aws-sdk/lib-dynamodb';
import { VersionMismatchError, NotFoundError } from '../errors/index.js';
import { Logger } from '../logger/index.js';
import { ConstructorItem, EntityBase, PrimaryKey } from '../entities/base.js';
import { FilterBuilder } from './filters.js';
import { KeyConditionBuilder } from './keyCondition.js';
import { UpdateExpression } from './updateExpression.js';
import { buildExclusiveStartKey, buildProjection, type ExclusiveStartKeyArgs } from './query.js';
import { Filter, QueryParams, UpdateOptions, DocClientKey, getKeyConfig, buildKey } from './utils.js';

const logger = new Logger({ module: import.meta.url });

type GetOptions = {
  consistentRead?: boolean;
};

type QueryOptions<E, PA> = {
  indexName?: string;
  filters?: Filter<E>[];
  scanIndexForward?: boolean;
  limit?: number;
  attributes?: PA;
  exclusiveStartKey?: ExclusiveStartKeyArgs['exclusiveStartKey'];
};

type QueryResult<E, PA> = {
  items: PA extends Array<keyof E> ? Pick<E, PA[number]>[] : E[];
  lastEvaluatedKey: DocClientKey | undefined;
};

type PutOptions = {
  skipVersionCheck?: boolean;
};

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
    if (items.length === 0 || items.length > 100) {
      throw new Error('Number of items must be between 1 and 100.');
    }

    try {
      const TransactItems = items.map((item) => ({ Put: this.buildPutItemInput(item) }));
      const response = await this.client.transactWrite({ TransactItems });
      logger.info({ response }, `Put transaction completed for ${TransactItems?.length} items`);

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

  private buildPutItemInput<E extends EntityBase>(item: E, options?: PutOptions): PutCommandInput {
    const entityConfig = item.getEntityConfig();
    const docClientKey = buildKey(entityConfig.table, entityConfig.keyGenerator(item));
    Object.assign(item, docClientKey);
    const currentVersion = item.versionNumber || 0;
    item.versionNumber = currentVersion + 1;

    const input: PutCommandInput = { TableName: entityConfig.table.tableName, Item: item, ReturnValues: 'NONE' };
    if (options?.skipVersionCheck) {
      return input;
    }

    input.ConditionExpression = 'versionNumber = :currentVersion OR attribute_not_exists(versionNumber)';
    input.ExpressionAttributeValues = { ':currentVersion': currentVersion };
    return input;
  }

  async updateItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    item: Partial<Omit<E, 'versionNumber'>>,
    options?: UpdateOptions<E>,
  ): Promise<E> {
    const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
    const { text, nameAliases, valueAliases } = new UpdateExpression({ item, options }).build();
    logger.info({ docClientKey, text, nameAliases, valueAliases }, 'Constructed update expression');

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

  async deleteItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    conditions?: Partial<E>,
  ): Promise<E | undefined> {
    try {
      const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
      const response = await this.client.delete({
        TableName: entityClass.entityConfig.table.tableName,
        Key: docClientKey,
        ReturnValues: 'ALL_OLD',
        Expected: this.buildDeleteConditions(conditions),
      });

      if (!response.Attributes) return undefined;
      return new entityClass(response.Attributes as E) as E;
    } catch (error) {
      if (error instanceof ConditionalCheckFailedException) {
        return undefined;
      }

      logger.error({ error }, 'Error in delete item request');
      throw error;
    }
  }

  private buildDeleteConditions<E>(conditions?: Partial<E>): DeleteCommandInput['Expected'] {
    if (!conditions) return undefined;

    const expected: DeleteCommandInput['Expected'] = {};
    for (const [cKey, cVal] of Object.entries(conditions)) {
      expected[cKey] = { Value: cVal };
    }
    return expected;
  }

  async query<E extends EntityBase, PA extends Array<keyof E> | undefined = undefined>(
    entityClass: EntityTarget<E>,
    queryParams: QueryParams,
    options?: QueryOptions<E, PA>,
  ): Promise<QueryResult<E, PA>> {
    const keyConfig = getKeyConfig(entityClass.entityConfig, options?.indexName);
    const keyCondition = new KeyConditionBuilder({ keyConfig, queryParams }).build();
    const projection = buildProjection(options?.attributes as string[]);
    const filter = new FilterBuilder<E>({ filters: options?.filters }).build();
    const exclusiveStartKey = buildExclusiveStartKey(entityClass.entityConfig, options);
    logger.debug({ keyConfig, exclusiveStartKey, keyCondition, projection, filter }, 'Derived query options');

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
      ExclusiveStartKey: exclusiveStartKey,
    });

    const items = (response.Items || []).map((item) => new entityClass(item as E)) as QueryResult<E, PA>['items'];
    return { items: items, lastEvaluatedKey: response.LastEvaluatedKey };
  }
}
