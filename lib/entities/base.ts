export type PrimaryKey = {
  partitionKey: string | number;
  sortKey?: string | number;
};

export type KeyConfig = {
  readonly pkName: string;
  readonly skName?: string;
};

export type Table = { readonly tableName: string } & KeyConfig;

export type ConstructorItem<E extends EntityBase> = Omit<E, 'versionNumber' | 'getEntityConfig'>;

//eslint-disable-next-line @typescript-eslint/no-explicit-any
type KeyGeneratorFunction = (...args: any[]) => PrimaryKey;

export type EntityConfig = {
  table: Table;
  keyGenerator: KeyGeneratorFunction;
  indexes?: Record<string, KeyConfig>;
};

export class EntityBase {
  createdOn!: number;
  createdBy!: string;
  updatedOn!: number;
  updatedBy!: string;
  versionNumber!: number;

  static entityConfig: EntityConfig;

  getEntityConfig(): EntityConfig {
    throw new Error('getEntityConfig is not implemented on the entity');
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  constructor(_item: ConstructorItem<EntityBase>) {}
}

/**
 * Decorators provide the ability to add/modify target class members.
 * {@link https://www.typescriptlang.org/docs/handbook/decorators.html#decorator-factories}
 *
 * @param config configuration object for the entity. It holds the table, indexes, and key generator functions.
 *
 * The provided configuration is attached to the target class as a static member {@this entityConfig} and also as a
 * getter function {@this getEntityConfig}. The static member and getter functions are used in dynamo mapper to get
 * access to the entity configuration while performing Dynamo DB operations.
 */
export function entity(config: EntityConfig) {
  return <E extends typeof EntityBase>(entityClass: E): E => {
    entityClass.entityConfig = config;
    entityClass.prototype.getEntityConfig = () => config;
    return entityClass;
  };
}
