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
import { Crypto } from './crypto.js';

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
  private static _cachedClient: DynamoDBDocument | undefined;
  private readonly client: DynamoDBDocument;
  private readonly crypto: Crypto;

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
    this.crypto = new Crypto();
  }

  async getItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    options?: GetOptions,
  ): Promise<E | undefined> {
    const response = await this.client.get({
      TableName: entityClass.entityConfig.table.tableName,
      Key: buildKey(entityClass.entityConfig.table, primaryKey),
      ConsistentRead: options?.consistentRead,
    });

    if (!response.Item) {
      return undefined;
    }

    const item = new entityClass(response.Item as E) as E;
    return await this.decryptItem(item);
  }

  async putItem<E extends EntityBase>(item: E, options?: PutOptions): Promise<E> {
    try {
      const input = await this.buildPutItemInput(item, options);
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
      const inputs = await Promise.all(items.map((item) => this.buildPutItemInput(item)));
      const TransactItems = inputs.map((input) => ({ Put: input }));
      await this.client.transactWrite({ TransactItems });
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

  async updateItem<E extends EntityBase>(
    entityClass: EntityTarget<E>,
    primaryKey: PrimaryKey,
    attributes: Partial<ConstructorItem<E>>,
    options?: UpdateOptions<E>,
  ): Promise<E> {
    const docClientKey = buildKey(entityClass.entityConfig.table, primaryKey);
    const encrypted = await this.encryptItem(attributes, entityClass.entityConfig.encryptedFields);
    const { text, nameAliases, valueAliases } = new UpdateExpression({ attributes: encrypted, options }).build();
    logger.info({ docClientKey, text, nameAliases }, 'Constructed update expression');

    try {
      const response = await this.client.update({
        TableName: entityClass.entityConfig.table.tableName,
        Key: docClientKey,
        UpdateExpression: text,
        ExpressionAttributeNames: nameAliases,
        ExpressionAttributeValues: valueAliases,
        ReturnValues: 'ALL_NEW',
      });

      const item = new entityClass(response.Attributes as E) as E;
      return await this.decryptItem(item);
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
      const response = await this.client.delete({
        TableName: entityClass.entityConfig.table.tableName,
        Key: buildKey(entityClass.entityConfig.table, primaryKey),
        ReturnValues: 'ALL_OLD',
        Expected: this.buildDeleteConditions(conditions),
      });

      if (!response.Attributes) return undefined;

      const item = new entityClass(response.Attributes as E) as E;
      return await this.decryptItem(item);
    } catch (error) {
      if (error instanceof ConditionalCheckFailedException) {
        return undefined;
      }

      logger.error({ error }, 'Error in delete item request');
      throw error;
    }
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

    const items = (response.Items || []).map((item) => new entityClass(item as E));
    const decrypted = await Promise.all(items.map((item) => this.decryptItem(item)));
    return { items: decrypted as QueryResult<E, PA>['items'], lastEvaluatedKey: response.LastEvaluatedKey };
  }

  private async decryptItem<E extends EntityBase>(item: E): Promise<E> {
    const { encryptedFields } = item.getEntityConfig();
    if (!encryptedFields || encryptedFields.length === 0) {
      return item;
    }

    await Promise.all(encryptedFields.map((field) => this.decryptField(item, field)));
    return item;
  }

  private async decryptField<E>(item: E, field: string): Promise<void> {
    const encryptedKey = `encrypted_${field}`;
    const cipherText = item[encryptedKey as keyof E] as string | undefined;
    if (!cipherText) {
      return;
    }

    const plainText = await this.crypto.decrypt(cipherText);
    item[field as keyof E] = JSON.parse(plainText) as never;
    delete item[encryptedKey as keyof E];
  }

  private async encryptField<E>(item: E, field: string): Promise<void> {
    const plainText = item[field as keyof E];
    if (!plainText) {
      return;
    }

    const cipherText = await this.crypto.encrypt(JSON.stringify(plainText));
    const encryptedKey = `encrypted_${field}`;
    item[encryptedKey as keyof E] = cipherText as never;
    delete item[field as keyof E];
  }

  private async encryptItem<T>(item: T, encryptedFields?: string[]): Promise<T> {
    if (!encryptedFields || encryptedFields.length === 0) {
      return item;
    }

    const transformed = { ...item };
    await Promise.all(encryptedFields.map((field) => this.encryptField(transformed, field)));
    return transformed;
  }

  private async buildPutItemInput<E extends EntityBase>(item: E, options?: PutOptions): Promise<PutCommandInput> {
    const entityConfig = item.getEntityConfig();
    const docClientKey = buildKey(entityConfig.table, entityConfig.keyGenerator(item));
    Object.assign(item, docClientKey);
    const currentVersion = item.versionNumber || 0;
    item.versionNumber = currentVersion + 1;

    const encrypted = await this.encryptItem(item, entityConfig.encryptedFields);
    const input: PutCommandInput = { TableName: entityConfig.table.tableName, Item: encrypted, ReturnValues: 'NONE' };
    if (options?.skipVersionCheck) {
      return input;
    }

    input.ConditionExpression = 'versionNumber = :currentVersion OR attribute_not_exists(versionNumber)';
    input.ExpressionAttributeValues = { ':currentVersion': currentVersion };
    return input;
  }

  private buildDeleteConditions<E>(conditions?: Partial<E>): DeleteCommandInput['Expected'] {
    if (!conditions) return undefined;

    const expected: DeleteCommandInput['Expected'] = {};
    for (const [cKey, cVal] of Object.entries(conditions)) {
      expected[cKey] = { Value: cVal };
    }
    return expected;
  }
}
